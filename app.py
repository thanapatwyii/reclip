import os
import time
import glob
import json
import shutil
import hashlib
import subprocess
import threading
from flask import Flask, request, jsonify, send_file, render_template

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True  # page edits apply without a restart
DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

SAVE_DIR = os.path.expanduser(os.environ.get("RECLIP_SAVE_DIR", "~/Movies/Personal/Misc"))
QUEUE_FILE = os.path.join(DOWNLOAD_DIR, "queue.json")
MAX_ATTEMPTS = int(os.environ.get("RECLIP_MAX_ATTEMPTS", 50))
STALL_SECONDS = int(os.environ.get("RECLIP_STALL_SECONDS", 180))

# Errors that retrying will never fix: stop after 3 tries instead of 50.
PERMANENT_ERRORS = ("Unsupported URL", "Video unavailable", "Private video", "not available",
                    "This video has been removed", "HTTP Error 404", "HTTP Error 410")

jobs = {}
jobs_lock = threading.RLock()
wake = threading.Event()  # nudges the queue runner when a job is added
ACTIVE = ("queued", "downloading", "retrying")


def save_queue():
    """Persist jobs so unfinished downloads survive a server restart."""
    with jobs_lock:
        data = {k: {f: v for f, v in list(j.items()) if f != "proc"} for k, j in list(jobs.items())}
        tmp = QUEUE_FILE + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(data, fh, indent=1)
        os.replace(tmp, QUEUE_FILE)


def load_queue():
    if not os.path.exists(QUEUE_FILE):
        return
    try:
        with open(QUEUE_FILE) as fh:
            jobs.update(json.load(fh))
    except (OSError, ValueError):
        return
    for job in jobs.values():
        if job["status"] == "downloading":
            job["status"] = "queued"  # it resumes when its turn comes


def job_key(url, format_choice, format_id):
    """Same video + same format = same job, so it resumes the same .part file."""
    raw = f"{url}|{format_choice}|{format_id or ''}"
    return hashlib.sha1(raw.encode()).hexdigest()[:10]


def partial_size(job_id):
    return sum(os.path.getsize(f) for f in glob.glob(os.path.join(DOWNLOAD_DIR, f"{job_id}.*"))
               if os.path.isfile(f))


def unique_path(path):
    base, ext = os.path.splitext(path)
    n = 2
    while os.path.exists(path):
        path = f"{base} ({n}){ext}"
        n += 1
    return path


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def read_progress(job, stdout):
    """Turn yt-dlp's progress lines into job["progress"] for the page's bar."""
    for line in stdout:
        line = line.strip()
        if line.startswith("PROG|"):
            _, vcodec, done, total, estimate, speed, eta = (line.split("|") + [""] * 7)[:7]
            done, total = _num(done), _num(total) or _num(estimate)
            job["progress"] = {
                "stage": "audio" if vcodec == "none" else "video",
                "percent": round(done / total * 100, 1) if done and total else None,
                "speed": _num(speed),
                "eta": _num(eta),
            }
        elif line.startswith(("[Merger]", "[ExtractAudio]", "[VideoConvertor]")):
            job["progress"] = {"stage": "finishing", "percent": 100}


def line_order():
    """Jobs waiting their turn, oldest first (shared by every tab)."""
    waiting = [(k, j) for k, j in list(jobs.items()) if j["status"] in ACTIVE]
    return [k for k, _ in sorted(waiting, key=lambda kv: kv[1].get("added", 0))]


def next_ready():
    now = time.time()
    for job_id in line_order():
        if jobs[job_id].get("next_try", 0) <= now:
            return job_id
    return None


def queue_runner():
    """Download ONE video at a time across all tabs, oldest first.

    A failed video goes back in line (after a short wait) so it never
    blocks the others; it resumes its partial file on its next turn.
    """
    while True:
        job_id = next_ready()
        if not job_id:
            wake.wait(5)
            wake.clear()
            continue
        job = jobs[job_id]
        job["attempts"] = job.get("attempts", 0) + 1
        job["status"] = "downloading"
        save_queue()
        if run_download(job_id, job["url"], job["format"], job.get("format_id")):
            continue
        if job["status"] == "cancelled":
            continue
        hopeless = any(p in (job.get("error") or "") for p in PERMANENT_ERRORS)
        if job["attempts"] >= MAX_ATTEMPTS or (hopeless and job["attempts"] >= 3):
            job["status"] = "error"
        else:
            job["status"] = "retrying"
            job["next_try"] = time.time() + min(10 * job["attempts"], 120)
        save_queue()


def start_worker(job_id):
    wake.set()


def parse_ytdlp_json(stdout):
    """Parse yt-dlp JSON output.

    With ``-j`` yt-dlp prints one JSON object per line. Some extractors
    emit multiple videos even with ``--no-playlist``, so stdout contains
    several objects and a plain ``json.loads`` raises "Extra data".
    Return the first valid object.
    """
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        return json.loads(line)
    raise ValueError("yt-dlp returned no data")


def run_download(job_id, url, format_choice, format_id):
    job = jobs[job_id]
    out_template = os.path.join(DOWNLOAD_DIR, f"{job_id}.%(ext)s")

    cmd = ["yt-dlp", "--no-playlist", "--continue", "--retries", "20",
           "--fragment-retries", "20", "--concurrent-fragments", "8", "--newline", "--progress-template",
           "download:PROG|%(info.vcodec)s|%(progress.downloaded_bytes)s|%(progress.total_bytes)s"
           "|%(progress.total_bytes_estimate)s|%(progress.speed)s|%(progress.eta)s",
           "-o", out_template]

    if format_choice == "audio":
        cmd += ["-x", "--audio-format", "mp3"]
    elif format_id:
        cmd += ["-f", f"{format_id}+bestaudio/best", "--merge-output-format", "mp4"]
    else:
        cmd += ["-f", "bestvideo+bestaudio/best", "--merge-output-format", "mp4"]

    cmd.append(url)

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        job["proc"] = proc
        job["progress"] = {"stage": "starting"}
        stderr_lines = []
        reader = threading.Thread(target=lambda: stderr_lines.extend(proc.stderr), daemon=True)
        reader.start()
        threading.Thread(target=read_progress, args=(job, proc.stdout), daemon=True).start()

        # No total time limit: only restart it when it barely moves
        # (under 1 MB in STALL_SECONDS), which also catches a slow trickle.
        window_size, window_start = partial_size(job_id), time.time()
        while proc.poll() is None:
            time.sleep(5)
            size = partial_size(job_id)
            job["bytes"] = size
            if (job.get("progress") or {}).get("stage") == "finishing":
                window_size, window_start = size, time.time()
            elif time.time() - window_start > STALL_SECONDS:
                if size - window_size < 1024 * 1024:
                    proc.kill()
                    proc.wait()
                    job["error"] = f"Almost no progress in {STALL_SECONDS}s, retrying"
                    return False
                window_size, window_start = size, time.time()
        reader.join(timeout=5)
        job.pop("proc", None)

        if proc.returncode != 0:
            lines = [l.strip() for l in stderr_lines if l.strip()]
            job["error"] = lines[-1] if lines else f"yt-dlp exited with {proc.returncode}"
            return False

        files = [f for f in glob.glob(os.path.join(DOWNLOAD_DIR, f"{job_id}.*"))
                 if not f.endswith((".part", ".ytdl"))]
        if not files:
            job["error"] = "Download completed but no file was found"
            return False

        want = ".mp3" if format_choice == "audio" else ".mp4"
        target = [f for f in files if f.endswith(want)]
        chosen = target[0] if target else files[0]

        for f in glob.glob(os.path.join(DOWNLOAD_DIR, f"{job_id}.*")):
            if f != chosen:
                try:
                    os.remove(f)
                except OSError:
                    pass

        ext = os.path.splitext(chosen)[1]
        title = job.get("title", "").strip()
        safe_title = "".join(c for c in title if c not in r'\/:*?"<>|').strip()[:100].strip()
        filename = f"{safe_title}{ext}" if safe_title else os.path.basename(chosen)

        # Save straight into ~/Downloads.
        os.makedirs(SAVE_DIR, exist_ok=True)
        dest = unique_path(os.path.join(SAVE_DIR, filename))
        shutil.move(chosen, dest)

        job["status"] = "done"
        job["file"] = dest
        job["filename"] = os.path.basename(dest)
        job["error"] = None
        save_queue()
        return True
    except Exception as e:
        job["error"] = str(e)
        return False
    finally:
        job.pop("proc", None)


def page_version():
    """Changes whenever the page or server code changes, so open tabs reload themselves."""
    here = os.path.dirname(os.path.abspath(__file__))
    stamp = "|".join(str(os.path.getmtime(os.path.join(here, f))) for f in ("app.py", "templates/index.html"))
    return hashlib.sha1(f"{BOOT}|{stamp}".encode()).hexdigest()[:10]


BOOT = str(time.time())


@app.route("/")
def index():
    return render_template("index.html", version=page_version())


@app.route("/api/version")
def version():
    return jsonify({"version": page_version()})


@app.route("/api/info", methods=["POST"])
def get_info():
    data = request.json
    url = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    cmd = ["yt-dlp", "--no-playlist", "-j", url]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return jsonify({"error": result.stderr.strip().split("\n")[-1]}), 400

        info = parse_ytdlp_json(result.stdout)

        # Build quality options — keep best format per resolution
        best_by_height = {}
        for f in info.get("formats", []):
            height = f.get("height")
            if height and f.get("vcodec", "none") != "none":
                tbr = f.get("tbr") or 0
                if height not in best_by_height or tbr > (best_by_height[height].get("tbr") or 0):
                    best_by_height[height] = f

        formats = []
        for height, f in best_by_height.items():
            formats.append({
                "id": f["format_id"],
                "label": f"{height}p",
                "height": height,
            })
        formats.sort(key=lambda x: x["height"], reverse=True)

        return jsonify({
            "title": info.get("title", ""),
            "thumbnail": info.get("thumbnail", ""),
            "duration": info.get("duration"),
            "uploader": info.get("uploader", ""),
            "formats": formats,
        })
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timed out fetching video info"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/playlist", methods=["POST"])
def get_playlist_info():
    data = request.json
    url = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    cmd = ["yt-dlp", "--flat-playlist", "-J", url]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return jsonify({"error": result.stderr.strip().split("\n")[-1]}), 400

        info = json.loads(result.stdout)
        entries = info.get("entries", [])
        urls = [entry.get("url") for entry in entries if entry.get("url")]
        return jsonify({"urls": urls})
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timed out fetching playlist info"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/download", methods=["POST"])
def start_download():
    data = request.json
    url = data.get("url", "").strip()
    format_choice = data.get("format", "video")
    format_id = data.get("format_id")
    title = data.get("title", "")

    if not url:
        return jsonify({"error": "No URL provided"}), 400

    job_id = job_key(url, format_choice, format_id)
    existing = jobs.get(job_id)
    if existing and existing["status"] in ("queued", "downloading", "retrying"):
        return jsonify({"job_id": job_id})  # another tab already has it
    if existing and existing["status"] == "done" and os.path.exists(existing.get("file", "")):
        return jsonify({"job_id": job_id})

    with jobs_lock:
        jobs[job_id] = {"status": "queued", "url": url, "title": title, "format": format_choice,
                        "format_id": format_id, "attempts": 0, "added": time.time()}
    save_queue()
    start_worker(job_id)

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def check_status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job_summary(job_id, job))


def job_summary(job_id, job):
    return {
        "id": job_id,
        "status": job["status"],
        "error": job.get("error"),
        "filename": job.get("filename"),
        "title": job.get("title"),
        "url": job.get("url"),
        "attempts": job.get("attempts", 0),
        "bytes": job.get("bytes", 0),
        "progress": job.get("progress") if job["status"] == "downloading" else None,
        "position": line_order().index(job_id) + 1 if job["status"] in ACTIVE else None,
    }


@app.route("/api/jobs")
def list_jobs():
    items = sorted(jobs.items(), key=lambda kv: kv[1].get("added", 0), reverse=True)
    return jsonify([job_summary(k, j) for k, j in items])


@app.route("/api/close-tab", methods=["POST"])
def close_tab():
    """Close the Chrome tab whose address ends in #tab-<id> (all its videos are done)."""
    tab = "".join(c for c in (request.json or {}).get("tab", "") if c.isalnum())
    if not tab:
        return jsonify({"error": "No tab id"}), 400
    script = f'''
    tell application "Google Chrome"
        repeat with w in windows
            repeat with t in (tabs of w)
                if URL of t ends with "#tab-{tab}" then close t
            end repeat
        end repeat
    end tell'''
    result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        return jsonify({"error": result.stderr.strip()}), 500
    return jsonify({"ok": True})


@app.route("/api/cancel/<job_id>", methods=["POST"])
def cancel_job(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    job["status"] = "cancelled"
    proc = job.get("proc")
    if proc:
        proc.kill()
    save_queue()
    return jsonify({"ok": True})


@app.route("/api/file/<job_id>")
def download_file(job_id):
    job = jobs.get(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "File not ready"}), 404
    return send_file(job["file"], as_attachment=True, download_name=job["filename"])


load_queue()
threading.Thread(target=queue_runner, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8899))
    host = os.environ.get("HOST", "127.0.0.1")
    app.run(host=host, port=port)

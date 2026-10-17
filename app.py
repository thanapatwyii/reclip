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
DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

SAVE_DIR = os.path.expanduser(os.environ.get("RECLIP_SAVE_DIR", "~/Downloads"))
QUEUE_FILE = os.path.join(DOWNLOAD_DIR, "queue.json")
MAX_ATTEMPTS = int(os.environ.get("RECLIP_MAX_ATTEMPTS", 50))
STALL_SECONDS = int(os.environ.get("RECLIP_STALL_SECONDS", 180))
MAX_PARALLEL = int(os.environ.get("RECLIP_PARALLEL", 4))

# Errors that retrying will never fix: stop after 3 tries instead of 50.
PERMANENT_ERRORS = ("Unsupported URL", "Video unavailable", "Private video", "not available",
                    "This video has been removed", "HTTP Error 404", "HTTP Error 410")

jobs = {}
jobs_lock = threading.RLock()
slots = threading.Semaphore(MAX_PARALLEL)


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
    for job_id, job in jobs.items():
        if job["status"] in ("queued", "downloading", "retrying"):
            start_worker(job_id)


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


def start_worker(job_id):
    thread = threading.Thread(target=download_until_done, args=(job_id,))
    thread.daemon = True
    thread.start()


def download_until_done(job_id):
    """Retry (resuming the partial file) until the download succeeds."""
    job = jobs[job_id]
    with slots:
        while job["status"] not in ("done", "cancelled"):
            job["attempts"] = job.get("attempts", 0) + 1
            job["status"] = "downloading"
            save_queue()
            if run_download(job_id, job["url"], job["format"], job.get("format_id")):
                return
            if job["status"] == "cancelled":
                return
            hopeless = any(p in (job.get("error") or "") for p in PERMANENT_ERRORS)
            if job["attempts"] >= MAX_ATTEMPTS or (hopeless and job["attempts"] >= 3):
                job["status"] = "error"
                save_queue()
                return
            job["status"] = "retrying"
            save_queue()
            time.sleep(min(10 * job["attempts"], 120))


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
           "--fragment-retries", "20", "-o", out_template]

    if format_choice == "audio":
        cmd += ["-x", "--audio-format", "mp3"]
    elif format_id:
        cmd += ["-f", f"{format_id}+bestaudio/best", "--merge-output-format", "mp4"]
    else:
        cmd += ["-f", "bestvideo+bestaudio/best", "--merge-output-format", "mp4"]

    cmd.append(url)

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        job["proc"] = proc
        stderr_lines = []
        reader = threading.Thread(target=lambda: stderr_lines.extend(proc.stderr), daemon=True)
        reader.start()

        # No total time limit: only kill it when the file stops growing.
        last_size, last_change = partial_size(job_id), time.time()
        while proc.poll() is None:
            time.sleep(5)
            size = partial_size(job_id)
            if size != last_size:
                last_size, last_change = size, time.time()
                job["bytes"] = size
            elif time.time() - last_change > STALL_SECONDS:
                proc.kill()
                proc.wait()
                job["error"] = f"Stalled for {STALL_SECONDS}s, retrying"
                return False
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


@app.route("/")
def index():
    return render_template("index.html")


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
    }


@app.route("/api/jobs")
def list_jobs():
    items = sorted(jobs.items(), key=lambda kv: kv[1].get("added", 0), reverse=True)
    return jsonify([job_summary(k, j) for k, j in items])


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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8899))
    host = os.environ.get("HOST", "127.0.0.1")
    app.run(host=host, port=port)

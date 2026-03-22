import streamlit as st
import subprocess
import os
import uuid
import shutil
import re
import tempfile
import json
from pathlib import Path
import multiprocessing

st.set_page_config(
    page_title="Video and Subtitle Merger",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="collapsed"
)


def init_state():
    if "eps" not in st.session_state:
        st.session_state.eps = []
    if "save_path" not in st.session_state:
        st.session_state.save_path = "/tmp/merged_videos"
    clean = []
    for ep in st.session_state.eps:
        if ep is None:
            continue
        if not isinstance(ep, dict):
            continue
        ep.setdefault("name", "")
        ep.setdefault("video", None)
        ep.setdefault("srt", None)
        ep.setdefault("job", None)
        clean.append(ep)
    st.session_state.eps = clean


init_state()

CPU_CORES = multiprocessing.cpu_count()
FF_THREADS = max(1, CPU_CORES - 1)
MAX_EPISODES = 40

CSS = (
    "<style>"
    "#MainMenu,footer,header{visibility:hidden}"
    ".block-container{padding:1rem 1rem 2rem;max-width:960px}"
    ".stApp{background:linear-gradient(135deg,#0f0c29,#302b63,#24243e)}"
    ".merger-card{"
    "background:rgba(255,255,255,.05);"
    "border:1px solid rgba(255,255,255,.1);"
    "border-radius:16px;padding:20px;margin-bottom:16px}"
    ".merger-title{"
    "font-size:14px;font-weight:700;"
    "color:rgba(255,255,255,.85);margin-bottom:12px}"
    ".hw-chip{"
    "display:inline-block;padding:3px 10px;"
    "border-radius:20px;font-size:11px;font-weight:700;margin:2px}"
    ".hw-on{"
    "background:rgba(16,185,129,.2);"
    "border:1px solid rgba(16,185,129,.4);color:#6ee7b7}"
    ".hw-off{"
    "background:rgba(255,255,255,.06);"
    "border:1px solid rgba(255,255,255,.1);"
    "color:rgba(255,255,255,.3)}"
    ".ep-card{"
    "background:rgba(255,255,255,.03);"
    "border:1.5px solid rgba(255,255,255,.08);"
    "border-radius:12px;padding:16px;margin-bottom:10px}"
    ".ep-done{"
    "border-color:rgba(16,185,129,.5)!important;"
    "background:rgba(16,185,129,.06)!important}"
    ".ep-error{"
    "border-color:rgba(255,59,48,.5)!important;"
    "background:rgba(255,59,48,.06)!important}"
    ".ep-running{"
    "border-color:rgba(99,102,241,.5)!important;"
    "background:rgba(99,102,241,.06)!important}"
    ".badge-ok{"
    "display:inline-block;padding:2px 8px;"
    "border-radius:20px;font-size:10px;font-weight:700;"
    "background:rgba(16,185,129,.15);"
    "border:1px solid rgba(16,185,129,.3);color:#6ee7b7}"
    ".mismatch-box{"
    "background:rgba(251,191,36,.12);"
    "border:1px solid rgba(251,191,36,.35);"
    "border-radius:8px;padding:8px 12px;"
    "color:#fde68a;font-size:12px;margin-top:6px;line-height:1.6}"
    ".info-box{"
    "background:rgba(99,102,241,.1);"
    "border:1px solid rgba(99,102,241,.25);"
    "border-radius:8px;padding:10px 14px;"
    "color:#a5b4fc;font-size:12px;margin-top:8px}"
    ".warn-box{"
    "background:rgba(255,59,48,.1);"
    "border:1px solid rgba(255,59,48,.3);"
    "border-radius:10px;padding:12px 16px;"
    "color:#ff6b6b;font-size:12px;margin-bottom:12px}"
    ".how-box{"
    "background:rgba(16,185,129,.07);"
    "border:1px solid rgba(16,185,129,.25);"
    "border-radius:12px;padding:14px 18px;"
    "color:#a7f3d0;font-size:13px;"
    "margin-bottom:16px;line-height:2}"
    ".dl-box{"
    "background:rgba(16,185,129,.1);"
    "border:2px solid rgba(16,185,129,.4);"
    "border-radius:10px;padding:12px 16px;"
    "color:#6ee7b7;font-size:12px;"
    "margin-top:8px;word-break:break-all}"
    ".batch-divider{"
    "text-align:center;color:rgba(255,255,255,.2);"
    "font-size:11px;margin:4px 0 14px}"
    ".sum-box{"
    "background:rgba(99,102,241,.08);"
    "border:1px solid rgba(99,102,241,.2);"
    "border-radius:10px;padding:12px 16px;"
    "font-size:12px;color:#a5b4fc;"
    "margin-top:10px;line-height:1.8}"
    ".upload-info{"
    "background:rgba(99,102,241,.08);"
    "border:1px solid rgba(99,102,241,.2);"
    "border-radius:8px;padding:6px 10px;"
    "color:#a5b4fc;font-size:11px;margin-top:4px}"
    "</style>"
)
st.markdown(CSS, unsafe_allow_html=True)


@st.cache_data(ttl=60)
def check_ffmpeg():
    try:
        subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            check=True,
            timeout=10
        )
        return True
    except Exception:
        return False


@st.cache_data(ttl=300)
def check_hw_accel():
    hw = {
        "nvenc": False,
        "qsv": False,
        "videotoolbox": False,
        "vaapi": False
    }
    try:
        r = subprocess.run(
            ["ffmpeg", "-encoders"],
            capture_output=True,
            text=True,
            timeout=10
        )
        out = r.stdout
        if "h264_nvenc" in out:
            hw["nvenc"] = True
        if "h264_qsv" in out:
            hw["qsv"] = True
        if "h264_videotoolbox" in out:
            hw["videotoolbox"] = True
        if "h264_vaapi" in out:
            hw["vaapi"] = True
    except Exception:
        pass
    return hw


def get_best_encoder():
    hw = check_hw_accel()
    if hw["nvenc"]:
        return "h264_nvenc", ["-preset", "p1", "-tune", "ll", "-b:v", "0", "-cq", "28"]
    if hw["videotoolbox"]:
        return "h264_videotoolbox", ["-q:v", "55", "-realtime", "1"]
    if hw["qsv"]:
        return "h264_qsv", ["-preset", "veryfast", "-global_quality", "28"]
    if hw["vaapi"]:
        return "h264_vaapi", ["-qp", "28"]
    return "libx264", [
        "-preset", "ultrafast",
        "-crf", "26",
        "-threads", str(FF_THREADS),
        "-tune", "fastdecode"
    ]


def format_file_size(size_bytes):
    if size_bytes < 1024:
        return str(size_bytes) + " B"
    if size_bytes < 1024 ** 2:
        return str(round(size_bytes / 1024, 1)) + " KB"
    if size_bytes < 1024 ** 3:
        return str(round(size_bytes / 1024 ** 2, 1)) + " MB"
    return str(round(size_bytes / 1024 ** 3, 2)) + " GB"


def get_uploaded_file_size(f):
    try:
        pos = f.tell()
        f.seek(0, 2)
        s = f.tell()
        f.seek(pos)
        return s
    except Exception:
        try:
            return f.size
        except Exception:
            return 0


def save_uploaded_file_chunked(uploaded_file, dest_path, chunk_size=64 * 1024 * 1024):
    uploaded_file.seek(0)
    total = 0
    with open(dest_path, "wb") as out:
        while True:
            chunk = uploaded_file.read(chunk_size)
            if not chunk:
                break
            out.write(chunk)
            total += len(chunk)
    return total


def unique_dest(folder, filename):
    dest = os.path.join(folder, filename)
    if not os.path.exists(dest):
        return dest
    stem, suffix = os.path.splitext(filename)
    return os.path.join(folder, stem + "_" + uuid.uuid4().hex[:4] + suffix)


def good(result, out_path):
    if not result or result.returncode != 0:
        return False
    if not os.path.exists(out_path):
        return False
    if os.path.getsize(out_path) < 10000:
        return False
    return True


def parse_srt(path):
    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]
    content = None
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc) as f:
                content = f.read()
            break
        except Exception:
            continue
    if not content:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    content = content.replace("\ufeff", "")
    content = content.replace("\r\n", "\n")
    content = content.replace("\r", "\n")
    entries = []
    for block in re.split(r"\n\s*\n", content.strip()):
        lines = block.strip().split("\n")
        if len(lines) < 2:
            continue
        time_match = None
        time_idx = -1
        for li, line in enumerate(lines):
            m = re.match(
                r"(\d{2}):(\d{2}):(\d{2})[,.](\d{3})\s*-->\s*"
                r"(\d{2}):(\d{2}):(\d{2})[,.](\d{3})",
                line.strip()
            )
            if m:
                time_match = m
                time_idx = li
                break
        if not time_match:
            continue
        g = time_match.groups()
        start = int(g[0]) * 3600 + int(g[1]) * 60 + int(g[2]) + int(g[3]) / 1000
        end = int(g[4]) * 3600 + int(g[5]) * 60 + int(g[6]) + int(g[7]) / 1000
        text = "\n".join(lines[time_idx + 1:])
        text = re.sub(r"<[^>]+>", "", text)
        text = re.sub(r"\{[^}]+\}", "", text).strip()
        if text:
            entries.append({"start": start, "end": end, "text": text})
    return entries


def fmt_srt_time(s):
    h = int(s // 3600)
    m = int((s % 3600) // 60)
    sc = int(s % 60)
    ms = int((s % 1) * 1000)
    return (
        str(h).zfill(2) + ":"
        + str(m).zfill(2) + ":"
        + str(sc).zfill(2) + ","
        + str(ms).zfill(3)
    )


def fmt_ass_time(s):
    h = int(s // 3600)
    m = int((s % 3600) // 60)
    sc = int(s % 60)
    cs = int((s % 1) * 100)
    return (
        str(h) + ":"
        + str(m).zfill(2) + ":"
        + str(sc).zfill(2) + "."
        + str(cs).zfill(2)
    )


def clean_srt(src, dst):
    entries = parse_srt(src)
    with open(dst, "w", encoding="utf-8") as f:
        for i, e in enumerate(entries, 1):
            f.write(str(i) + "\n")
            f.write(fmt_srt_time(e["start"]) + " --> " + fmt_srt_time(e["end"]) + "\n")
            f.write(e["text"] + "\n\n")
    return len(entries)


def create_ass(srt_path, ass_path, w=1920, h=1080):
    entries = parse_srt(srt_path)
    fs = max(int(h * 0.045), 24)
    mv = int(h * 0.06)
    mlr = int(w * 0.05)
    header = (
        "[Script Info]\n"
        "ScriptType: v4.00+\n"
        "PlayResX: " + str(w) + "\n"
        "PlayResY: " + str(h) + "\n"
        "WrapStyle: 0\n"
        "ScaledBorderAndShadow: yes\n\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
        "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
        "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding\n"
        "Style: Default,Arial," + str(fs) + ","
        "&H00FFFFFF,&H000000FF,&H00000000,&H96000000,"
        "0,0,0,0,100,100,0,0,1,2,1,2,"
        + str(mlr) + "," + str(mlr) + "," + str(mv) + ",1\n\n"
        "[Events]\n"
        "Format: Layer, Start, End, Style, Name, "
        "MarginL, MarginR, MarginV, Effect, Text\n"
    )
    lines = []
    for e in entries:
        txt = e["text"].replace("\n", "\\N").replace("{", "").replace("}", "")
        lines.append(
            "Dialogue: 0,"
            + fmt_ass_time(e["start"]) + ","
            + fmt_ass_time(e["end"])
            + ",Default,,0,0,0,," + txt + "\n"
        )
    with open(ass_path, "w", encoding="utf-8") as f:
        f.write(header)
        f.writelines(lines)
    return len(entries)


def get_video_info(path):
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_streams", "-show_format", path],
            capture_output=True,
            text=True,
            timeout=30
        )
        if r.returncode == 0:
            data = json.loads(r.stdout)
            w, h, dur = 1920, 1080, 0
            for s in data.get("streams", []):
                if s.get("codec_type") == "video":
                    w = int(s.get("width", 1920))
                    h = int(s.get("height", 1080))
                    dur = float(s.get("duration", 0))
            if dur == 0:
                dur = float(data.get("format", {}).get("duration", 0))
            return {"width": w, "height": h, "duration": dur}
    except Exception:
        pass
    return {"width": 1920, "height": 1080, "duration": 0}


def escape_path(p):
    p = p.replace("\\", "/")
    if len(p) >= 2 and p[1] == ":":
        p = p[0] + "\\:" + p[2:]
    return p.replace("'", "\\'")


def safe_get_job(ep):
    if not isinstance(ep, dict):
        return {}
    j = ep.get("job")
    if not isinstance(j, dict):
        return {}
    return j


def process_episode(
    video_file, video_name, srt_file, srt_name,
    ep_name, merge_type, dl_folder, progress_cb=None
):
    work_dir = None
    out_file = None

    def upd(pct, msg):
        if progress_cb:
            try:
                progress_cb(pct, msg)
            except Exception:
                pass

    try:
        upd(1, "Preparing workspace...")
        work_dir = tempfile.mkdtemp(prefix="ep_")

        v_ext = Path(video_name).suffix.lower() or ".mp4"
        s_ext = Path(srt_name).suffix.lower() or ".srt"
        v_path = os.path.join(work_dir, "video" + v_ext)
        s_path = os.path.join(work_dir, "srt" + s_ext)

        upd(2, "Writing video to disk...")
        v_size = save_uploaded_file_chunked(video_file, v_path)
        upd(5, "Video written: " + format_file_size(v_size))

        upd(6, "Writing subtitle...")
        save_uploaded_file_chunked(srt_file, s_path)

        if os.path.getsize(v_path) < 1000:
            raise ValueError("Video file too small — upload may have failed")
        if os.path.getsize(s_path) < 5:
            raise ValueError("Subtitle file too small")

        entries = parse_srt(s_path)
        if not entries:
            raise ValueError("No subtitle entries found in SRT file")
        upd(8, "Parsed " + str(len(entries)) + " subtitle entries")

        info = get_video_info(v_path)
        sub_ext = Path(s_path).suffix.lower()
        safe = re.sub(r"[<>:\"/\\|?*\x00-\x1f]", "_", ep_name)
        safe = re.sub(r"_+", "_", safe).strip("_") or "episode"

        encoder, enc_args = get_best_encoder()
        os.makedirs(dl_folder, exist_ok=True)

        def run_ff_prog(cmd):
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1
                )
                dur = info["duration"]
                stderr_lines = []
                for line in proc.stderr:
                    stderr_lines.append(line)
                    m = re.search(r"time=(\d+):(\d+):([\d.]+)", line)
                    if m and dur > 0:
                        el = (
                            int(m.group(1)) * 3600
                            + int(m.group(2)) * 60
                            + float(m.group(3))
                        )
                        pct = int(15 + min(el / dur, 1.0) * 75)
                        upd(pct, "Encoding... " + str(int(min(el / dur, 1.0) * 100)) + "%")
                proc.wait(timeout=86400)

                class R:
                    returncode = proc.returncode
                    stderr = "".join(stderr_lines)
                    stdout = ""

                return R()
            except subprocess.TimeoutExpired:
                proc.kill()
                return None
            except Exception as ex:
                print("[run_ff_prog] " + str(ex))
                return None

        if merge_type == "hard":
            out_file = unique_dest(dl_folder, safe + ".mp4")
            upd(10, "Encoding with " + encoder + "...")
            success = False
            last_err = ""

            try:
                ass = os.path.join(work_dir, "styled.ass")
                if create_ass(s_path, ass, info["width"], info["height"]) > 0:
                    ae = escape_path(os.path.abspath(ass))
                    cmd = (
                        ["ffmpeg", "-y", "-i", os.path.abspath(v_path),
                         "-vf", "ass='" + ae + "'",
                         "-c:v", encoder]
                        + enc_args
                        + ["-c:a", "copy", "-movflags", "+faststart",
                           os.path.abspath(out_file)]
                    )
                    r = run_ff_prog(cmd)
                    if good(r, out_file):
                        success = True
                    else:
                        last_err = r.stderr[-300:] if r and r.stderr else "ASS burn failed"
                        if os.path.exists(out_file):
                            os.remove(out_file)
            except Exception as ex:
                last_err = str(ex)
                if out_file and os.path.exists(out_file):
                    os.remove(out_file)

            if not success:
                try:
                    su = s_path
                    if sub_ext == ".srt":
                        cs = os.path.join(work_dir, "clean.srt")
                        clean_srt(s_path, cs)
                        su = cs
                    se = escape_path(os.path.abspath(su))
                    if sub_ext in (".ass", ".ssa"):
                        vf = "ass='" + se + "'"
                    else:
                        vf = "subtitles='" + se + "'"
                    cmd = (
                        ["ffmpeg", "-y", "-i", os.path.abspath(v_path),
                         "-vf", vf, "-c:v", encoder]
                        + enc_args
                        + ["-c:a", "copy", "-movflags", "+faststart",
                           os.path.abspath(out_file)]
                    )
                    r = run_ff_prog(cmd)
                    if good(r, out_file):
                        success = True
                    else:
                        last_err = r.stderr[-300:] if r and r.stderr else "filter failed"
                        if os.path.exists(out_file):
                            os.remove(out_file)
                except Exception as ex:
                    last_err = str(ex)
                    if out_file and os.path.exists(out_file):
                        os.remove(out_file)

            if not success:
                raise RuntimeError("All hard-sub methods failed: " + last_err[:300])

        else:
            success = False
            last_err = ""
            upd(15, "Soft sub — stream copy...")

            out_mkv = unique_dest(dl_folder, safe + ".mkv")
            try:
                sub_codec = "ass" if sub_ext in (".ass", ".ssa") else "srt"
                r = subprocess.run(
                    ["ffmpeg", "-y",
                     "-i", os.path.abspath(v_path),
                     "-i", os.path.abspath(s_path),
                     "-map", "0:v", "-map", "0:a?", "-map", "1:0",
                     "-c:v", "copy", "-c:a", "copy", "-c:s", sub_codec,
                     "-metadata:s:s:0", "language=eng",
                     "-disposition:s:0", "default",
                     out_mkv],
                    capture_output=True,
                    text=True,
                    timeout=86400
                )
                if good(r, out_mkv):
                    out_file = out_mkv
                    success = True
                    upd(90, "Stream copy done...")
                else:
                    last_err = r.stderr[-300:] if r.stderr else "mkv failed"
                    if os.path.exists(out_mkv):
                        os.remove(out_mkv)
            except Exception as ex:
                last_err = str(ex)
                if os.path.exists(out_mkv):
                    os.remove(out_mkv)

            if not success:
                upd(40, "Trying MP4 soft sub...")
                out_mp4 = unique_dest(dl_folder, safe + ".mp4")
                try:
                    cs = os.path.join(work_dir, "clean.srt")
                    clean_srt(s_path, cs)
                    r = subprocess.run(
                        ["ffmpeg", "-y",
                         "-i", os.path.abspath(v_path),
                         "-i", cs,
                         "-map", "0:v", "-map", "0:a?", "-map", "1:0",
                         "-c:v", "copy", "-c:a", "copy", "-c:s", "mov_text",
                         "-metadata:s:s:0", "language=eng",
                         "-disposition:s:0", "default",
                         "-movflags", "+faststart",
                         os.path.abspath(out_mp4)],
                        capture_output=True,
                        text=True,
                        timeout=86400
                    )
                    if good(r, out_mp4):
                        out_file = out_mp4
                        success = True
                        upd(90, "Done...")
                    else:
                        last_err = r.stderr[-300:] if r.stderr else "mp4 failed"
                        if os.path.exists(out_mp4):
                            os.remove(out_mp4)
                except Exception as ex:
                    last_err = str(ex)

            if not success:
                raise RuntimeError("Soft-sub failed: " + last_err[:300])

        size_bytes = os.path.getsize(out_file)
        upd(100, "Done! " + format_file_size(size_bytes))
        return {
            "success": True,
            "path": out_file,
            "size_mb": round(size_bytes / 1024 / 1024, 1),
            "size_str": format_file_size(size_bytes),
            "filename": os.path.basename(out_file),
        }

    except Exception as exc:
        import traceback
        traceback.print_exc()
        msg = str(exc)
        if out_file and os.path.exists(out_file):
            try:
                os.remove(out_file)
            except Exception:
                pass
        upd(0, "Error: " + msg[:180])
        return {"success": False, "error": msg}

    finally:
        if work_dir and os.path.isdir(work_dir):
            shutil.rmtree(work_dir, ignore_errors=True)


def extract_number(filename):
    stem = Path(filename).stem
    for pat in [r"[Ee][Pp]?(\d+)", r"[Ee](\d+)"]:
        m = re.search(pat, stem)
        if m:
            return int(m.group(1))
    nums = re.findall(r"\d+", stem)
    return int(nums[-1]) if nums else None


def check_match(vname, sname):
    vn = extract_number(vname)
    sn = extract_number(sname)
    if vn is None and sn is None:
        return "unknown"
    if vn is None or sn is None:
        return "unknown"
    return "ok" if vn == sn else "mismatch"


hw = check_hw_accel()
ffmpeg_ok = check_ffmpeg()
enc_name, _ = get_best_encoder()

chips_html = "".join(
    '<span class="hw-chip ' + ("hw-on" if v else "hw-off") + '">'
    + ("⚡" if v else "○") + " " + k.upper() + "</span>"
    for k, v in hw.items()
)

st.markdown(
    '<div style="text-align:center;padding:24px 20px 16px;'
    'background:rgba(255,255,255,.05);'
    'border:1px solid rgba(255,255,255,.1);'
    'border-radius:20px;margin-bottom:20px">'
    '<div style="font-size:46px;margin-bottom:8px">🎬</div>'
    '<h1 style="font-size:24px;font-weight:800;margin:0 0 4px">'
    'Video &amp; Subtitle Merger</h1>'
    '<p style="color:rgba(255,255,255,.5);font-size:13px;margin:0 0 6px">'
    'Merge individually or in batch &nbsp;&bull;&nbsp; Up to '
    + str(MAX_EPISODES) +
    ' episodes &nbsp;&bull;&nbsp; '
    'Files up to <strong style="color:#6ee7b7">10 GB</strong></p>'
    '<div style="margin-top:8px">' + chips_html + "</div>"
    "</div>",
    unsafe_allow_html=True,
)

if not ffmpeg_ok:
    st.markdown(
        '<div class="warn-box">'
        "⚠️ FFmpeg not found! "
        "Ensure packages.txt contains ffmpeg"
        "</div>",
        unsafe_allow_html=True,
    )

st.markdown(
    '<div class="how-box">'
    "<strong>How this app works</strong><br>"
    "1. Upload your video and subtitle files below<br>"
    "2. Click Merge — the app processes files on the server<br>"
    "3. Click the Download button that appears<br>"
    '<span style="color:rgba(167,243,208,.6);font-size:11px">'
    "Files are processed on the server. "
    "Download button sends the file to your computer."
    "</span>"
    "</div>",
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="merger-card">'
    '<div class="merger-title">Subtitle Mode</div>',
    unsafe_allow_html=True,
)

mode = st.radio(
    "Subtitle mode",
    options=["hard", "soft"],
    format_func=lambda x: (
        "Hard — Burned-in subtitles (re-encodes video)"
        if x == "hard"
        else "Soft — Selectable track, no re-encode (FASTEST)"
    ),
    label_visibility="collapsed",
    horizontal=True,
)

if mode == "hard":
    st.markdown(
        '<div class="info-box">Encoder: <strong>'
        + enc_name +
        "</strong></div>",
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        '<div class="info-box">'
        "Stream copy — even a 10 GB file finishes in seconds!"
        "</div>",
        unsafe_allow_html=True,
    )

st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="merger-card">', unsafe_allow_html=True)
ep_count = len(st.session_state.eps)
st.markdown(
    '<div class="merger-title">Episodes '
    '<span style="background:rgba(99,102,241,.2);color:#a5b4fc;'
    'font-size:11px;padding:2px 10px;border-radius:20px;font-weight:700">'
    + str(ep_count) + " / " + str(MAX_EPISODES) +
    "</span></div>",
    unsafe_allow_html=True,
)

tb1, tb2, tb3, tb4, tb5 = st.columns([2, 1, 2, 2, 2])
with tb1:
    if st.button(
        "+ Add Episode",
        use_container_width=True,
        disabled=ep_count >= MAX_EPISODES
    ):
        st.session_state.eps.append({
            "name": "", "video": None, "srt": None, "job": None
        })
        st.rerun()

with tb2:
    n_bulk = st.number_input(
        "N",
        min_value=1,
        max_value=MAX_EPISODES,
        value=5,
        label_visibility="collapsed"
    )

with tb3:
    if st.button(
        "Add " + str(int(n_bulk)) + " Episodes",
        use_container_width=True,
        disabled=ep_count >= MAX_EPISODES
    ):
        to_add = min(int(n_bulk), MAX_EPISODES - ep_count)
        start = ep_count + 1
        for k in range(to_add):
            st.session_state.eps.append({
                "name": "EP" + str(start + k).zfill(2),
                "video": None,
                "srt": None,
                "job": None,
            })
        st.rerun()

with tb4:
    with st.expander("Bulk Rename"):
        bp = st.text_input("Prefix", value="EP", key="bp")
        bs = st.number_input("Start", min_value=1, value=1, key="bs")
        bpd = st.number_input("Pad", min_value=1, max_value=4, value=2, key="bpd")
        bsf = st.text_input("Suffix", value="", key="bsf")
        if st.button("Apply to all", use_container_width=True):
            for k in range(len(st.session_state.eps)):
                st.session_state.eps[k]["name"] = (
                    bp + str(int(bs) + k).zfill(int(bpd)) + bsf
                )
            st.rerun()

with tb5:
    if st.button("Clear All", use_container_width=True, disabled=ep_count == 0):
        st.session_state.eps = []
        st.rerun()

st.divider()

eps_to_delete = []

for i in range(len(st.session_state.eps)):
    ep = st.session_state.eps[i]
    if ep is None or not isinstance(ep, dict):
        continue

    job = safe_get_job(ep)
    status = job.get("status", "")
    is_done = status == "completed"
    is_err = status == "error"
    is_running = status == "processing"

    with st.container():
        if is_done:
            card_cls = " ep-done"
        elif is_err:
            card_cls = " ep-error"
        elif is_running:
            card_cls = " ep-running"
        else:
            card_cls = ""

        st.markdown(
            '<div class="ep-card' + card_cls + '">',
            unsafe_allow_html=True
        )

        h1, h2, h3, h4 = st.columns([4, 1, 1, 1])
        with h1:
            new_name = st.text_input(
                "Name " + str(i + 1),
                value=ep.get("name", "") or ("EP" + str(i + 1).zfill(2)),
                key="epname_" + str(i),
                label_visibility="collapsed",
                placeholder="EP" + str(i + 1).zfill(2),
            )
            st.session_state.eps[i]["name"] = new_name

        with h2:
            if st.button("Up", key="up_" + str(i), disabled=(i == 0)):
                tmp = st.session_state.eps[i - 1]
                st.session_state.eps[i - 1] = st.session_state.eps[i]
                st.session_state.eps[i] = tmp
                st.rerun()

        with h3:
            last_idx = len(st.session_state.eps) - 1
            if st.button("Dn", key="dn_" + str(i), disabled=(i == last_idx)):
                tmp = st.session_state.eps[i + 1]
                st.session_state.eps[i + 1] = st.session_state.eps[i]
                st.session_state.eps[i] = tmp
                st.rerun()

        with h4:
            if st.button("X", key="del_" + str(i)):
                eps_to_delete.append(i)

        f1, f2 = st.columns(2)

        with f1:
            vfile = st.file_uploader(
                "Video (up to 10 GB)",
                type=["mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v"],
                key="vup_" + str(i),
            )
            if vfile is not None:
                st.session_state.eps[i]["video"] = vfile
                if status in ("completed", "error"):
                    st.session_state.eps[i]["job"] = None
                try:
                    sz = get_uploaded_file_size(vfile)
                    if sz > 0:
                        st.markdown(
                            '<div class="upload-info">'
                            + vfile.name
                            + " — "
                            + format_file_size(sz)
                            + "</div>",
                            unsafe_allow_html=True,
                        )
                except Exception:
                    pass

        with f2:
            sfile = st.file_uploader(
                "Subtitle (.srt / .ass)",
                type=["srt", "ass", "ssa", "vtt", "sub"],
                key="sup_" + str(i),
            )
            if sfile is not None:
                st.session_state.eps[i]["srt"] = sfile
                if status in ("completed", "error"):
                    st.session_state.eps[i]["job"] = None

        v_up = st.session_state.eps[i].get("video")
        s_up = st.session_state.eps[i].get("srt")

        if v_up and s_up:
            try:
                match = check_match(v_up.name, s_up.name)
                if match == "mismatch":
                    vn = extract_number(v_up.name)
                    sn = extract_number(s_up.name)
                    st.markdown(
                        '<div class="mismatch-box">'
                        "Number mismatch: Video #"
                        + str(vn)
                        + " vs Subtitle #"
                        + str(sn)
                        + "<br>"
                        + v_up.name
                        + " vs "
                        + s_up.name
                        + "</div>",
                        unsafe_allow_html=True,
                    )
                elif match == "ok":
                    vn = extract_number(v_up.name)
                    st.markdown(
                        '<span class="badge-ok">#' + str(vn) + " matched</span>",
                        unsafe_allow_html=True,
                    )
            except Exception:
                pass

        job = safe_get_job(st.session_state.eps[i])
        status = job.get("status", "")

        if status == "completed":
            out_path = job.get("path", "")
            size_str = job.get("size_str", str(job.get("size_mb", "?")) + " MB")
            st.success("Processing complete — " + size_str)
            if out_path and os.path.isfile(out_path):
                fname = os.path.basename(out_path)
                file_size = os.path.getsize(out_path)
                st.markdown(
                    '<div class="dl-box">'
                    "Your file is ready! Click the button below to download."
                    "</div>",
                    unsafe_allow_html=True,
                )
                try:
                    with open(out_path, "rb") as fh:
                        st.download_button(
                            label="Download " + fname + " (" + format_file_size(file_size) + ")",
                            data=fh,
                            file_name=fname,
                            mime="video/mp4",
                            key="dl_" + str(i) + "_" + uuid.uuid4().hex[:6],
                            use_container_width=True,
                        )
                except Exception as ex:
                    st.warning("Could not prepare download: " + str(ex))

        elif status == "error":
            err_msg = str(job.get("msg", "Failed"))
            st.error("Error: " + err_msg)

        elif status == "processing":
            pct_val = job.get("pct", 0)
            msg_val = str(job.get("msg", "Processing..."))
            st.progress(pct_val / 100, text=msg_val)

        can_merge = bool(v_up and s_up and not is_running)

        if is_err:
            btn_lbl = "Retry"
        elif is_done:
            btn_lbl = "Re-merge"
        else:
            btn_lbl = "Merge This Episode"

        if st.button(
            btn_lbl,
            key="merge_ep_" + str(i),
            disabled=not can_merge,
            use_container_width=True,
        ):
            v_obj = st.session_state.eps[i]["video"]
            s_obj = st.session_state.eps[i]["srt"]
            v_name = v_obj.name
            s_name = s_obj.name
            ep_name = st.session_state.eps[i].get("name") or ("Episode_" + str(i + 1))

            proceed = True
            if check_match(v_name, s_name) == "mismatch":
                proceed = st.checkbox(
                    "Mismatch detected — tick to proceed anyway",
                    key="mismatch_ok_" + str(i),
                )

            if proceed:
                st.session_state.eps[i]["job"] = {
                    "status": "processing",
                    "pct": 1,
                    "msg": "Writing file to disk...",
                    "path": None,
                }

                prog = st.progress(0, text="Starting...")

                def cb(pct, msg, _p=prog):
                    try:
                        _p.progress(min(pct, 100) / 100, text=msg[:120])
                    except Exception:
                        pass

                try:
                    v_obj.seek(0)
                    s_obj.seek(0)
                except Exception:
                    pass

                work_out = "/tmp/merged_videos"
                os.makedirs(work_out, exist_ok=True)

                result = process_episode(
                    v_obj, v_name, s_obj, s_name,
                    ep_name, mode, work_out,
                    progress_cb=cb,
                )

                if result["success"]:
                    new_status = "completed"
                    new_pct = 100
                    new_msg = result.get("size_str", "Done")
                else:
                    new_status = "error"
                    new_pct = 0
                    new_msg = result.get("error", "Failed")

                st.session_state.eps[i]["job"] = {
                    "status": new_status,
                    "pct": new_pct,
                    "msg": new_msg,
                    "path": result.get("path", ""),
                    "size_mb": result.get("size_mb", 0),
                    "size_str": result.get("size_str", ""),
                }
                st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)

if eps_to_delete:
    for idx in sorted(eps_to_delete, reverse=True):
        if 0 <= idx < len(st.session_state.eps):
            st.session_state.eps.pop(idx)
    st.rerun()

if not st.session_state.eps:
    st.markdown(
        '<div style="text-align:center;padding:28px;'
        'color:rgba(255,255,255,.25);font-size:13px">'
        "Click + Add Episode to get started"
        "</div>",
        unsafe_allow_html=True,
    )

st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="merger-card">', unsafe_allow_html=True)
st.markdown(
    '<div class="batch-divider">batch merge all at once</div>',
    unsafe_allow_html=True,
)

valid_eps = [
    (i, ep)
    for i, ep in enumerate(st.session_state.eps)
    if ep is not None
    and isinstance(ep, dict)
    and ep.get("video")
    and ep.get("srt")
]
n_valid = len(valid_eps)

if n_valid > 0:
    batch_label = "Merge All " + str(n_valid) + " Episode" + ("s" if n_valid != 1 else "")
else:
    batch_label = "Merge All Episodes"

if st.button(
    batch_label,
    disabled=n_valid == 0,
    use_container_width=True,
    type="primary",
):
    work_out = "/tmp/merged_videos"
    os.makedirs(work_out, exist_ok=True)
    overall = st.progress(0, text="Starting batch...")
    ok_cnt = 0
    fail_cnt = 0

    for step, (i, ep) in enumerate(valid_eps):
        overall.progress(
            step / n_valid,
            text="Episode " + str(step + 1) + " of " + str(n_valid) + "...",
        )
        ep_name = ep.get("name") or ("Episode_" + str(i + 1))
        v_obj = ep["video"]
        s_obj = ep["srt"]

        try:
            v_obj.seek(0)
            s_obj.seek(0)
        except Exception:
            pass

        holder = st.empty()

        def cb(pct, msg, h=holder, name=ep_name):
            try:
                h.progress(min(pct, 100) / 100, text=name + ": " + msg)
            except Exception:
                pass

        result = process_episode(
            v_obj, v_obj.name, s_obj, s_obj.name,
            ep_name, mode, work_out,
            progress_cb=cb,
        )

        if result["success"]:
            new_status = "completed"
            new_pct = 100
            new_msg = result.get("size_str", "Done")
            ok_cnt += 1
            holder.success(ep_name + " — " + result.get("size_str", ""))
        else:
            new_status = "error"
            new_pct = 0
            new_msg = result.get("error", "Failed")
            fail_cnt += 1
            holder.error(ep_name + " — " + result.get("error", "Failed"))

        st.session_state.eps[i]["job"] = {
            "status": new_status,
            "pct": new_pct,
            "msg": new_msg,
            "path": result.get("path", ""),
            "size_mb": result.get("size_mb", 0),
            "size_str": result.get("size_str", ""),
        }

    overall.progress(
        1.0,
        text="Done — " + str(ok_cnt) + " succeeded, " + str(fail_cnt) + " failed",
    )
    st.markdown(
        '<div class="sum-box">'
        "Batch complete: "
        + str(ok_cnt) + " completed, "
        + str(fail_cnt) + " failed"
        "</div>",
        unsafe_allow_html=True,
    )
    st.rerun()

st.markdown("</div>", unsafe_allow_html=True)

done_eps = []
for ep in st.session_state.eps:
    if ep is None or not isinstance(ep, dict):
        continue
    job = ep.get("job")
    if not isinstance(job, dict):
        continue
    if job.get("status") != "completed":
        continue
    path = job.get("path", "")
    if not path or not os.path.isfile(str(path)):
        continue
    done_eps.append(ep)

if done_eps:
    st.markdown(
        '<div class="merger-card">'
        '<div class="merger-title">Download All Completed Files</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div style="color:rgba(255,255,255,.45);font-size:12px;margin-bottom:12px">'
        "Click each button to download the merged file."
        "</div>",
        unsafe_allow_html=True,
    )
    for ep in done_eps:
        job = ep["job"]
        path = job["path"]
        fname = os.path.basename(path)
        try:
            file_size = os.path.getsize(path)
            ep_name = ep.get("name") or fname
            label = ep_name + " — " + format_file_size(file_size)
            with open(path, "rb") as fh:
                st.download_button(
                    label=label,
                    data=fh,
                    file_name=fname,
                    mime="video/mp4",
                    key="dlall_" + fname + "_" + uuid.uuid4().hex[:6],
                    use_container_width=True,
                )
        except Exception as ex:
            st.warning("Could not prepare " + fname + ": " + str(ex))

    st.markdown("</div>", unsafe_allow_html=True)
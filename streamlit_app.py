import streamlit as st
import subprocess
import os
import uuid
import shutil
import re
import tempfile
import json
import traceback
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
    if "ep_video_bytes" not in st.session_state:
        st.session_state.ep_video_bytes = {}
    if "ep_video_names" not in st.session_state:
        st.session_state.ep_video_names = {}
    if "ep_srt_bytes" not in st.session_state:
        st.session_state.ep_srt_bytes = {}
    if "ep_srt_names" not in st.session_state:
        st.session_state.ep_srt_names = {}
    if "debug_logs" not in st.session_state:
        st.session_state.debug_logs = {}
    clean = []
    for ep in st.session_state.eps:
        if ep is None:
            continue
        if not isinstance(ep, dict):
            continue
        ep.setdefault("name", "")
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
    ".error-summary{"
    "background:rgba(255,59,48,.12);"
    "border:1px solid rgba(255,59,48,.4);"
    "border-radius:8px;padding:10px 14px;"
    "color:#ff6b6b;font-size:12px;margin-top:6px;"
    "font-weight:600}"
    ".log-step{"
    "background:rgba(0,0,0,.35);"
    "border-left:3px solid rgba(99,102,241,.6);"
    "border-radius:0 6px 6px 0;"
    "padding:6px 10px;margin:3px 0;"
    "color:#c7d2fe;font-size:11px;font-family:monospace}"
    ".log-step-ok{"
    "border-left-color:rgba(16,185,129,.6)!important;"
    "color:#a7f3d0!important}"
    ".log-step-err{"
    "border-left-color:rgba(255,59,48,.7)!important;"
    "color:#fca5a5!important}"
    ".ffmpeg-out{"
    "background:rgba(0,0,0,.5);"
    "border:1px solid rgba(255,255,255,.08);"
    "border-radius:6px;padding:10px;"
    "color:#e5e7eb;font-size:10px;"
    "font-family:monospace;white-space:pre-wrap;"
    "max-height:300px;overflow-y:auto;"
    "margin-top:6px}"
    "</style>"
)
st.markdown(CSS, unsafe_allow_html=True)


@st.cache_data(ttl=60)
def check_ffmpeg():
    try:
        r = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if r.returncode == 0:
            first_line = r.stdout.split("\n")[0] if r.stdout else "unknown"
            return True, first_line
        return False, r.stderr[:200]
    except FileNotFoundError:
        return False, "ffmpeg binary not found in PATH"
    except Exception as ex:
        return False, str(ex)


@st.cache_data(ttl=300)
def check_hw_accel():
    hw = {"nvenc": False, "qsv": False, "videotoolbox": False, "vaapi": False}
    try:
        r = subprocess.run(
            ["ffmpeg", "-encoders"],
            capture_output=True, text=True, timeout=10
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
        return f"{size_bytes} B"
    if size_bytes < 1024 ** 2:
        return f"{round(size_bytes / 1024, 1)} KB"
    if size_bytes < 1024 ** 3:
        return f"{round(size_bytes / 1024 ** 2, 1)} MB"
    return f"{round(size_bytes / 1024 ** 3, 2)} GB"


def save_bytes_to_file(data_bytes, dest_path):
    with open(dest_path, "wb") as f:
        f.write(data_bytes)
    return len(data_bytes)


def unique_dest(folder, filename):
    dest = os.path.join(folder, filename)
    if not os.path.exists(dest):
        return dest
    stem, suffix = os.path.splitext(filename)
    return os.path.join(folder, stem + "_" + uuid.uuid4().hex[:4] + suffix)


def good(result, out_path):
    if result is None:
        return False
    if result.returncode != 0:
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
    content = content.replace("\r\n", "\n").replace("\r", "\n")
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
    return f"{h:02d}:{m:02d}:{sc:02d},{ms:03d}"


def fmt_ass_time(s):
    h = int(s // 3600)
    m = int((s % 3600) // 60)
    sc = int(s % 60)
    cs = int((s % 1) * 100)
    return f"{h}:{m:02d}:{sc:02d}.{cs:02d}"


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
    if not entries:
        return 0
    fs = max(int(h * 0.045), 24)
    mv = int(h * 0.06)
    mlr = int(w * 0.05)
    header = (
        "[Script Info]\n"
        "ScriptType: v4.00+\n"
        f"PlayResX: {w}\n"
        f"PlayResY: {h}\n"
        "WrapStyle: 0\n"
        "ScaledBorderAndShadow: yes\n\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
        "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
        "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding\n"
        f"Style: Default,Arial,{fs},"
        "&H00FFFFFF,&H000000FF,&H00000000,&H96000000,"
        f"0,0,0,0,100,100,0,0,1,2,1,2,{mlr},{mlr},{mv},1\n\n"
        "[Events]\n"
        "Format: Layer, Start, End, Style, Name, "
        "MarginL, MarginR, MarginV, Effect, Text\n"
    )
    lines = []
    for e in entries:
        txt = e["text"].replace("\n", "\\N").replace("{", "").replace("}", "")
        lines.append(
            f"Dialogue: 0,{fmt_ass_time(e['start'])},{fmt_ass_time(e['end'])}"
            f",Default,,0,0,0,,{txt}\n"
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
            capture_output=True, text=True, timeout=30
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
        else:
            return {
                "width": 1920, "height": 1080, "duration": 0,
                "probe_error": r.stderr[:300]
            }
    except Exception as ex:
        return {"width": 1920, "height": 1080, "duration": 0, "probe_error": str(ex)}


def safe_get_job(ep):
    if not isinstance(ep, dict):
        return {}
    j = ep.get("job")
    if not isinstance(j, dict):
        return {}
    return j


def run_ffmpeg_logged(cmd, duration, progress_cb, start_pct, end_pct):
    """
    Run ffmpeg, stream stderr, report progress, and return
    (returncode, full_stderr_text).
    """
    full_stderr = []
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        for line in proc.stderr:
            full_stderr.append(line)
            if duration and duration > 0:
                m = re.search(r"time=(\d+):(\d+):([\d.]+)", line)
                if m:
                    elapsed = (
                        int(m.group(1)) * 3600
                        + int(m.group(2)) * 60
                        + float(m.group(3))
                    )
                    ratio = min(elapsed / duration, 1.0)
                    pct = int(start_pct + ratio * (end_pct - start_pct))
                    if progress_cb:
                        progress_cb(pct, f"Encoding… {int(ratio * 100)}%")

        proc.wait(timeout=86400)
        return proc.returncode, "".join(full_stderr)

    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except Exception:
            pass
        return -1, "FFmpeg timed out after 24 h"
    except Exception as ex:
        return -1, f"Popen exception: {traceback.format_exc()}"


def process_episode_from_bytes(
    video_bytes, video_name,
    srt_bytes, srt_name,
    ep_name, merge_type, dl_folder,
    progress_cb=None
):
    """
    Returns dict with keys:
      success, path, size_mb, size_str, filename   (on success)
      success, error, error_detail, steps          (on failure)
    steps is a list of {"label", "ok", "detail"} dicts.
    """
    work_dir = None
    out_file = None
    steps = []          # audit trail shown to user
    ffmpeg_logs = []    # raw ffmpeg stderr lines for every attempt

    def upd(pct, msg):
        if progress_cb:
            try:
                progress_cb(pct, msg)
            except Exception:
                pass

    def add_step(label, ok, detail=""):
        steps.append({"label": label, "ok": ok, "detail": str(detail)})

    try:
        # ── workspace ──────────────────────────────────────────────────────
        upd(1, "Preparing workspace…")
        work_dir = tempfile.mkdtemp(prefix="ep_")
        add_step("Created temp directory", True, work_dir)

        v_ext = Path(video_name).suffix.lower() or ".mp4"
        s_ext = Path(srt_name).suffix.lower() or ".srt"
        v_path = os.path.join(work_dir, "video" + v_ext)
        s_path = os.path.join(work_dir, "subtitle" + s_ext)

        # ── write files ────────────────────────────────────────────────────
        upd(2, "Writing video to disk…")
        save_bytes_to_file(video_bytes, v_path)
        v_size = os.path.getsize(v_path)
        add_step(
            f"Video written: {video_name}",
            v_size >= 1000,
            f"{format_file_size(v_size)} @ {v_path}"
        )
        if v_size < 1000:
            raise ValueError(
                f"Video file is only {v_size} bytes — upload appears corrupt or incomplete."
            )

        upd(6, "Writing subtitle…")
        save_bytes_to_file(srt_bytes, s_path)
        s_size = os.path.getsize(s_path)
        add_step(
            f"Subtitle written: {srt_name}",
            s_size >= 5,
            f"{format_file_size(s_size)} @ {s_path}"
        )
        if s_size < 5:
            raise ValueError(
                f"Subtitle file is only {s_size} bytes — upload appears corrupt."
            )

        # ── parse subtitles ────────────────────────────────────────────────
        upd(7, "Parsing subtitles…")
        try:
            entries = parse_srt(s_path)
        except Exception as ex:
            add_step("Parse SRT", False, str(ex))
            raise ValueError(f"Could not parse subtitle file: {ex}")

        add_step(
            f"SRT parsed: {len(entries)} entries",
            len(entries) > 0,
            "First entry: " + (
                f"{entries[0]['start']:.1f}s → \"{entries[0]['text'][:60]}\""
                if entries else "none"
            )
        )
        if not entries:
            raise ValueError(
                "No subtitle entries found. "
                "Check the file is a valid .srt with proper timestamps."
            )

        # ── probe video ────────────────────────────────────────────────────
        upd(9, "Probing video…")
        info = get_video_info(v_path)
        probe_ok = "probe_error" not in info
        add_step(
            f"Video probe: {info['width']}x{info['height']}, "
            f"{info['duration']:.1f}s",
            probe_ok,
            info.get("probe_error", "OK")
        )
        if not probe_ok:
            # non-fatal — we still try to encode
            upd(9, f"Probe warning: {info.get('probe_error','')[:80]}")

        safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", ep_name)
        safe = re.sub(r"_+", "_", safe).strip("_") or "episode"
        encoder, enc_args = get_best_encoder()
        add_step(f"Encoder selected: {encoder}", True)
        os.makedirs(dl_folder, exist_ok=True)

        # ══════════════════════════════════════════════════════════════════
        #  HARD SUB
        # ══════════════════════════════════════════════════════════════════
        if merge_type == "hard":
            out_file = unique_dest(dl_folder, safe + ".mp4")
            success = False

            # ── Method 1 : ASS burn-in ─────────────────────────────────
            try:
                ass_path = os.path.join(work_dir, "styled.ass")
                n_ass = create_ass(s_path, ass_path, info["width"], info["height"])
                add_step(f"ASS created: {n_ass} entries", n_ass > 0)

                if n_ass > 0:
                    upd(12, f"Hard sub (ASS) — encoder: {encoder}")
                    ass_abs = os.path.abspath(ass_path)
                    ass_esc = ass_abs.replace("\\", "\\\\").replace(":", "\\:")
                    cmd = (
                        ["ffmpeg", "-y", "-i", os.path.abspath(v_path),
                         "-vf", f"ass={ass_esc}",
                         "-c:v", encoder]
                        + enc_args
                        + ["-c:a", "copy", "-movflags", "+faststart",
                           os.path.abspath(out_file)]
                    )
                    add_step("FFmpeg cmd (ASS)", True, " ".join(cmd))
                    upd(13, "Running FFmpeg — method 1/3 (ASS)…")
                    rc, stderr = run_ffmpeg_logged(
                        cmd, info["duration"], upd, 15, 88
                    )
                    ffmpeg_logs.append(
                        f"=== Method 1: ASS burn-in ===\n"
                        f"CMD: {' '.join(cmd)}\n"
                        f"RC: {rc}\n{stderr}"
                    )
                    if good(type("R", (), {"returncode": rc})(), out_file):
                        success = True
                        add_step("Method 1 (ASS burn-in) SUCCESS", True)
                    else:
                        # extract meaningful lines from stderr
                        err_lines = [
                            l for l in stderr.splitlines()
                            if any(k in l.lower() for k in
                                   ["error", "invalid", "failed", "no such",
                                    "unable", "cannot", "could not"])
                        ]
                        add_step(
                            "Method 1 (ASS burn-in) FAILED",
                            False,
                            "\n".join(err_lines[-8:]) or stderr[-400:]
                        )
                        if os.path.exists(out_file):
                            os.remove(out_file)
            except Exception as ex:
                add_step("Method 1 (ASS) exception", False, traceback.format_exc())
                if out_file and os.path.exists(out_file):
                    try:
                        os.remove(out_file)
                    except Exception:
                        pass

            # ── Method 2 : subtitles filter ────────────────────────────
            if not success:
                try:
                    cs_path = os.path.join(work_dir, "clean.srt")
                    n_clean = clean_srt(s_path, cs_path)
                    add_step(f"Cleaned SRT: {n_clean} entries", n_clean > 0)

                    cs_abs = os.path.abspath(cs_path)
                    cs_esc = cs_abs.replace("\\", "\\\\").replace(":", "\\:")
                    cmd = (
                        ["ffmpeg", "-y", "-i", os.path.abspath(v_path),
                         "-vf", f"subtitles={cs_esc}",
                         "-c:v", encoder]
                        + enc_args
                        + ["-c:a", "copy", "-movflags", "+faststart",
                           os.path.abspath(out_file)]
                    )
                    add_step("FFmpeg cmd (subtitles filter)", True, " ".join(cmd))
                    upd(15, "Running FFmpeg — method 2/3 (subtitles filter)…")
                    rc, stderr = run_ffmpeg_logged(
                        cmd, info["duration"], upd, 16, 88
                    )
                    ffmpeg_logs.append(
                        f"=== Method 2: subtitles filter ===\n"
                        f"CMD: {' '.join(cmd)}\n"
                        f"RC: {rc}\n{stderr}"
                    )
                    if good(type("R", (), {"returncode": rc})(), out_file):
                        success = True
                        add_step("Method 2 (subtitles filter) SUCCESS", True)
                    else:
                        err_lines = [
                            l for l in stderr.splitlines()
                            if any(k in l.lower() for k in
                                   ["error", "invalid", "failed", "no such",
                                    "unable", "cannot", "could not"])
                        ]
                        add_step(
                            "Method 2 (subtitles filter) FAILED",
                            False,
                            "\n".join(err_lines[-8:]) or stderr[-400:]
                        )
                        if os.path.exists(out_file):
                            os.remove(out_file)
                except Exception as ex:
                    add_step("Method 2 exception", False, traceback.format_exc())
                    if out_file and os.path.exists(out_file):
                        try:
                            os.remove(out_file)
                        except Exception:
                            pass

            # ── Method 3 : quoted path fallback ───────────────────────
            if not success:
                try:
                    simple_srt = os.path.join(work_dir, "sub.srt")
                    shutil.copy2(cs_path if os.path.exists(
                        os.path.join(work_dir, "clean.srt")
                    ) else s_path, simple_srt)

                    cmd = (
                        ["ffmpeg", "-y", "-i", os.path.abspath(v_path),
                         "-vf", f"subtitles='{simple_srt}'",
                         "-c:v", encoder]
                        + enc_args
                        + ["-c:a", "copy", "-movflags", "+faststart",
                           os.path.abspath(out_file)]
                    )
                    add_step("FFmpeg cmd (fallback quoted)", True, " ".join(cmd))
                    upd(20, "Running FFmpeg — method 3/3 (fallback)…")
                    rc, stderr = run_ffmpeg_logged(
                        cmd, info["duration"], upd, 22, 88
                    )
                    ffmpeg_logs.append(
                        f"=== Method 3: fallback quoted ===\n"
                        f"CMD: {' '.join(cmd)}\n"
                        f"RC: {rc}\n{stderr}"
                    )
                    if good(type("R", (), {"returncode": rc})(), out_file):
                        success = True
                        add_step("Method 3 (fallback) SUCCESS", True)
                    else:
                        err_lines = [
                            l for l in stderr.splitlines()
                            if any(k in l.lower() for k in
                                   ["error", "invalid", "failed", "no such",
                                    "unable", "cannot", "could not"])
                        ]
                        add_step(
                            "Method 3 (fallback) FAILED",
                            False,
                            "\n".join(err_lines[-8:]) or stderr[-400:]
                        )
                        if os.path.exists(out_file):
                            os.remove(out_file)
                except Exception as ex:
                    add_step("Method 3 exception", False, traceback.format_exc())
                    if out_file and os.path.exists(out_file):
                        try:
                            os.remove(out_file)
                        except Exception:
                            pass

            if not success:
                raise RuntimeError(
                    "All 3 hard-sub methods failed — see steps below for details."
                )

        # ══════════════════════════════════════════════════════════════════
        #  SOFT SUB
        # ══════════════════════════════════════════════════════════════════
        else:
            success = False
            upd(15, "Soft sub — stream copy…")

            # ── Method 1 : MKV ────────────────────────────────────────
            out_mkv = unique_dest(dl_folder, safe + ".mkv")
            try:
                sub_codec = "ass" if s_ext in (".ass", ".ssa") else "srt"
                cmd = [
                    "ffmpeg", "-y",
                    "-i", os.path.abspath(v_path),
                    "-i", os.path.abspath(s_path),
                    "-map", "0:v", "-map", "0:a?", "-map", "1:0",
                    "-c:v", "copy", "-c:a", "copy", "-c:s", sub_codec,
                    "-metadata:s:s:0", "language=eng",
                    "-disposition:s:0", "default",
                    out_mkv
                ]
                add_step("FFmpeg cmd (MKV soft)", True, " ".join(cmd))
                upd(20, "Running FFmpeg — MKV stream copy…")
                rc, stderr = run_ffmpeg_logged(cmd, info["duration"], upd, 20, 88)
                ffmpeg_logs.append(
                    f"=== Soft Method 1: MKV ===\nCMD: {' '.join(cmd)}\nRC: {rc}\n{stderr}"
                )
                if good(type("R", (), {"returncode": rc})(), out_mkv):
                    out_file = out_mkv
                    success = True
                    add_step("Soft Method 1 (MKV) SUCCESS", True)
                else:
                    err_lines = [
                        l for l in stderr.splitlines()
                        if any(k in l.lower() for k in
                               ["error", "invalid", "failed", "no such",
                                "unable", "cannot", "could not"])
                    ]
                    add_step(
                        "Soft Method 1 (MKV) FAILED",
                        False,
                        "\n".join(err_lines[-8:]) or stderr[-400:]
                    )
                    if os.path.exists(out_mkv):
                        os.remove(out_mkv)
            except Exception as ex:
                add_step("Soft Method 1 exception", False, traceback.format_exc())
                if os.path.exists(out_mkv):
                    try:
                        os.remove(out_mkv)
                    except Exception:
                        pass

            # ── Method 2 : MP4 mov_text ───────────────────────────────
            if not success:
                upd(40, "Trying MP4 soft sub…")
                out_mp4 = unique_dest(dl_folder, safe + ".mp4")
                try:
                    cs_path = os.path.join(work_dir, "clean.srt")
                    clean_srt(s_path, cs_path)
                    cmd = [
                        "ffmpeg", "-y",
                        "-i", os.path.abspath(v_path),
                        "-i", cs_path,
                        "-map", "0:v", "-map", "0:a?", "-map", "1:0",
                        "-c:v", "copy", "-c:a", "copy", "-c:s", "mov_text",
                        "-metadata:s:s:0", "language=eng",
                        "-disposition:s:0", "default",
                        "-movflags", "+faststart",
                        os.path.abspath(out_mp4)
                    ]
                    add_step("FFmpeg cmd (MP4 soft)", True, " ".join(cmd))
                    upd(45, "Running FFmpeg — MP4 mov_text…")
                    rc, stderr = run_ffmpeg_logged(cmd, info["duration"], upd, 45, 88)
                    ffmpeg_logs.append(
                        f"=== Soft Method 2: MP4 mov_text ===\n"
                        f"CMD: {' '.join(cmd)}\nRC: {rc}\n{stderr}"
                    )
                    if good(type("R", (), {"returncode": rc})(), out_mp4):
                        out_file = out_mp4
                        success = True
                        add_step("Soft Method 2 (MP4) SUCCESS", True)
                    else:
                        err_lines = [
                            l for l in stderr.splitlines()
                            if any(k in l.lower() for k in
                                   ["error", "invalid", "failed", "no such",
                                    "unable", "cannot", "could not"])
                        ]
                        add_step(
                            "Soft Method 2 (MP4) FAILED",
                            False,
                            "\n".join(err_lines[-8:]) or stderr[-400:]
                        )
                        if os.path.exists(out_mp4):
                            os.remove(out_mp4)
                except Exception as ex:
                    add_step("Soft Method 2 exception", False, traceback.format_exc())

            if not success:
                raise RuntimeError(
                    "Both soft-sub methods failed — see steps below."
                )

        # ── success ────────────────────────────────────────────────────────
        size_bytes = os.path.getsize(out_file)
        size_str = format_file_size(size_bytes)
        upd(100, f"Done! {size_str}")
        add_step(f"Output file: {os.path.basename(out_file)}", True, size_str)

        return {
            "success": True,
            "path": out_file,
            "size_mb": round(size_bytes / 1024 / 1024, 1),
            "size_str": size_str,
            "filename": os.path.basename(out_file),
            "steps": steps,
            "ffmpeg_logs": ffmpeg_logs,
        }

    except Exception as exc:
        tb_str = traceback.format_exc()
        add_step(f"FATAL: {exc}", False, tb_str)
        upd(0, f"Failed: {str(exc)[:120]}")

        if out_file and os.path.exists(out_file):
            try:
                os.remove(out_file)
            except Exception:
                pass

        return {
            "success": False,
            "error": str(exc),
            "error_detail": tb_str,
            "steps": steps,
            "ffmpeg_logs": ffmpeg_logs,
        }

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


def render_steps(steps):
    """Render the step-by-step audit trail."""
    if not steps:
        return
    for s in steps:
        cls = "log-step-ok" if s["ok"] else "log-step-err"
        icon = "✓" if s["ok"] else "✗"
        label_html = (
            f'<div class="log-step {cls}">'
            f'{icon} {s["label"]}'
            + (f'<br><span style="opacity:.7">{s["detail"]}</span>'
               if s["detail"] else "")
            + "</div>"
        )
        st.markdown(label_html, unsafe_allow_html=True)


def render_ffmpeg_logs(logs):
    if not logs:
        return
    combined = "\n\n".join(logs)
    st.markdown(
        f'<div class="ffmpeg-out">{combined}</div>',
        unsafe_allow_html=True
    )


# ── header ─────────────────────────────────────────────────────────────────────
hw = check_hw_accel()
ffmpeg_ok, ffmpeg_ver = check_ffmpeg()
enc_name, _ = get_best_encoder()

chips_html = "".join(
    f'<span class="hw-chip {"hw-on" if v else "hw-off"}">'
    f'{"⚡" if v else "○"} {k.upper()}</span>'
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
    f'Merge individually or in batch &nbsp;&bull;&nbsp; Up to {MAX_EPISODES} episodes'
    ' &nbsp;&bull;&nbsp; Files up to '
    '<strong style="color:#6ee7b7">10 GB</strong></p>'
    f'<div style="margin-top:8px">{chips_html}</div>'
    '</div>',
    unsafe_allow_html=True,
)

if not ffmpeg_ok:
    st.markdown(
        f'<div class="warn-box">'
        f'⚠️ FFmpeg not found!<br>'
        f'<code>{ffmpeg_ver}</code><br>'
        f'Make sure <strong>packages.txt</strong> contains <code>ffmpeg</code>'
        f'</div>',
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        f'<div class="info-box">✓ {ffmpeg_ver}</div>',
        unsafe_allow_html=True,
    )

st.markdown(
    '<div class="how-box">'
    '<strong>How this app works</strong><br>'
    '1. Add episode slots, upload video + subtitle for each<br>'
    '2. Click <strong>Merge This Episode</strong> (or Merge All)<br>'
    '3. Click the Download button when it appears<br>'
    '<span style="color:rgba(167,243,208,.6);font-size:11px">'
    'All processing happens on the server. '
    'If a merge fails, detailed error steps are shown below the episode.'
    '</span>'
    '</div>',
    unsafe_allow_html=True,
)

# ── mode ───────────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="merger-card"><div class="merger-title">Subtitle Mode</div>',
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
        f'<div class="info-box">Encoder: <strong>{enc_name}</strong> '
        f'— {FF_THREADS} threads</div>',
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        '<div class="info-box">'
        'Stream copy — even a 10 GB file finishes in seconds!'
        '</div>',
        unsafe_allow_html=True,
    )
st.markdown("</div>", unsafe_allow_html=True)

# ── episode list ───────────────────────────────────────────────────────────────
st.markdown('<div class="merger-card">', unsafe_allow_html=True)
ep_count = len(st.session_state.eps)
st.markdown(
    f'<div class="merger-title">Episodes '
    f'<span style="background:rgba(99,102,241,.2);color:#a5b4fc;'
    f'font-size:11px;padding:2px 10px;border-radius:20px;font-weight:700">'
    f'{ep_count} / {MAX_EPISODES}</span></div>',
    unsafe_allow_html=True,
)

tb1, tb2, tb3, tb4, tb5 = st.columns([2, 1, 2, 2, 2])
with tb1:
    if st.button(
        "+ Add Episode", use_container_width=True,
        disabled=ep_count >= MAX_EPISODES
    ):
        st.session_state.eps.append({"name": "", "job": None})
        st.rerun()

with tb2:
    n_bulk = st.number_input(
        "N", min_value=1, max_value=MAX_EPISODES,
        value=5, label_visibility="collapsed"
    )

with tb3:
    if st.button(
        f"Add {int(n_bulk)} Episodes", use_container_width=True,
        disabled=ep_count >= MAX_EPISODES
    ):
        to_add = min(int(n_bulk), MAX_EPISODES - ep_count)
        start = ep_count + 1
        for k in range(to_add):
            st.session_state.eps.append({
                "name": f"EP{str(start + k).zfill(2)}",
                "job": None,
            })
        st.rerun()

with tb4:
    with st.expander("Bulk Rename"):
        bp  = st.text_input("Prefix", value="EP", key="bp")
        bs  = st.number_input("Start", min_value=1, value=1, key="bs")
        bpd = st.number_input("Pad", min_value=1, max_value=4, value=2, key="bpd")
        bsf = st.text_input("Suffix", value="", key="bsf")
        if st.button("Apply to all", use_container_width=True):
            for k in range(len(st.session_state.eps)):
                st.session_state.eps[k]["name"] = (
                    bp + str(int(bs) + k).zfill(int(bpd)) + bsf
                )
            st.rerun()

with tb5:
    if st.button(
        "Clear All", use_container_width=True,
        disabled=ep_count == 0
    ):
        st.session_state.eps = []
        st.session_state.ep_video_bytes = {}
        st.session_state.ep_video_names = {}
        st.session_state.ep_srt_bytes   = {}
        st.session_state.ep_srt_names   = {}
        st.session_state.debug_logs     = {}
        st.rerun()

st.divider()

eps_to_delete = []

for i in range(len(st.session_state.eps)):
    ep = st.session_state.eps[i]
    if ep is None or not isinstance(ep, dict):
        continue

    job      = safe_get_job(ep)
    status   = job.get("status", "")
    is_done  = status == "completed"
    is_err   = status == "error"
    is_run   = status == "processing"

    card_cls = (
        " ep-done"    if is_done else
        " ep-error"   if is_err  else
        " ep-running" if is_run  else ""
    )

    st.markdown(f'<div class="ep-card{card_cls}">', unsafe_allow_html=True)

    # ── header row ──────────────────────────────────────────────────────
    h1, h2, h3, h4 = st.columns([4, 1, 1, 1])
    with h1:
        new_name = st.text_input(
            f"Name {i+1}",
            value=ep.get("name", "") or f"EP{str(i+1).zfill(2)}",
            key=f"epname_{i}",
            label_visibility="collapsed",
            placeholder=f"EP{str(i+1).zfill(2)}",
        )
        st.session_state.eps[i]["name"] = new_name

    with h2:
        if st.button("Up", key=f"up_{i}", disabled=(i == 0)):
            for d in [
                st.session_state.ep_video_bytes,
                st.session_state.ep_video_names,
                st.session_state.ep_srt_bytes,
                st.session_state.ep_srt_names,
            ]:
                d[i], d[i-1] = d.get(i-1), d.get(i)
            (st.session_state.eps[i],
             st.session_state.eps[i-1]) = (
                st.session_state.eps[i-1],
                st.session_state.eps[i]
            )
            st.rerun()

    with h3:
        last_idx = len(st.session_state.eps) - 1
        if st.button("Dn", key=f"dn_{i}", disabled=(i == last_idx)):
            for d in [
                st.session_state.ep_video_bytes,
                st.session_state.ep_video_names,
                st.session_state.ep_srt_bytes,
                st.session_state.ep_srt_names,
            ]:
                d[i], d[i+1] = d.get(i+1), d.get(i)
            (st.session_state.eps[i],
             st.session_state.eps[i+1]) = (
                st.session_state.eps[i+1],
                st.session_state.eps[i]
            )
            st.rerun()

    with h4:
        if st.button("✕", key=f"del_{i}"):
            eps_to_delete.append(i)

    # ── uploaders ───────────────────────────────────────────────────────
    f1, f2 = st.columns(2)

    with f1:
        vfile = st.file_uploader(
            "Video (up to 10 GB)",
            type=["mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v"],
            key=f"vup_{i}",
        )
        if vfile is not None:
            vfile.seek(0)
            vb = vfile.read()
            st.session_state.ep_video_bytes[i] = vb
            st.session_state.ep_video_names[i] = vfile.name
            if status in ("completed", "error"):
                st.session_state.eps[i]["job"] = None
            st.markdown(
                f'<div class="upload-info">'
                f'✓ {vfile.name} — {format_file_size(len(vb))}'
                f'</div>',
                unsafe_allow_html=True,
            )
        elif i in st.session_state.ep_video_bytes:
            vb   = st.session_state.ep_video_bytes[i]
            vnm  = st.session_state.ep_video_names.get(i, "video")
            st.markdown(
                f'<div class="upload-info">'
                f'✓ {vnm} — {format_file_size(len(vb))}'
                f'</div>',
                unsafe_allow_html=True,
            )

    with f2:
        sfile = st.file_uploader(
            "Subtitle (.srt / .ass)",
            type=["srt", "ass", "ssa", "vtt", "sub"],
            key=f"sup_{i}",
        )
        if sfile is not None:
            sfile.seek(0)
            sb = sfile.read()
            st.session_state.ep_srt_bytes[i] = sb
            st.session_state.ep_srt_names[i] = sfile.name
            if status in ("completed", "error"):
                st.session_state.eps[i]["job"] = None
            st.markdown(
                f'<div class="upload-info">'
                f'✓ {sfile.name} — {format_file_size(len(sb))}'
                f'</div>',
                unsafe_allow_html=True,
            )
        elif i in st.session_state.ep_srt_bytes:
            sb   = st.session_state.ep_srt_bytes[i]
            snm  = st.session_state.ep_srt_names.get(i, "subtitle")
            st.markdown(
                f'<div class="upload-info">'
                f'✓ {snm} — {format_file_size(len(sb))}'
                f'</div>',
                unsafe_allow_html=True,
            )

    has_video = bool(st.session_state.ep_video_bytes.get(i))
    has_srt   = bool(st.session_state.ep_srt_bytes.get(i))
    v_nm      = st.session_state.ep_video_names.get(i, "")
    s_nm      = st.session_state.ep_srt_names.get(i, "")

    # ── number-match badge ──────────────────────────────────────────────
    if has_video and has_srt and v_nm and s_nm:
        try:
            match = check_match(v_nm, s_nm)
            if match == "mismatch":
                vn = extract_number(v_nm)
                sn = extract_number(s_nm)
                st.markdown(
                    f'<div class="mismatch-box">'
                    f'⚠ Number mismatch — Video #{vn} vs Subtitle #{sn}<br>'
                    f'{v_nm} &nbsp;↔&nbsp; {s_nm}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            elif match == "ok":
                st.markdown(
                    f'<span class="badge-ok">#{extract_number(v_nm)} matched</span>',
                    unsafe_allow_html=True,
                )
        except Exception:
            pass

    # ── job status ──────────────────────────────────────────────────────
    job    = safe_get_job(st.session_state.eps[i])
    status = job.get("status", "")

    if status == "completed":
        out_path = job.get("path", "")
        size_str = job.get("size_str", f"{job.get('size_mb','?')} MB")
        st.success(f"✅ Done — {size_str}")
        if out_path and os.path.isfile(out_path):
            fname     = os.path.basename(out_path)
            file_size = os.path.getsize(out_path)
            st.markdown(
                '<div class="dl-box">'
                'Your file is ready — click below to download.'
                '</div>',
                unsafe_allow_html=True,
            )
            with open(out_path, "rb") as fh:
                st.download_button(
                    label=f"⬇ Download {fname} ({format_file_size(file_size)})",
                    data=fh,
                    file_name=fname,
                    mime="video/mp4",
                    key=f"dl_{i}_{uuid.uuid4().hex[:6]}",
                    use_container_width=True,
                )
        # show steps in expander even on success
        steps_data = job.get("steps", [])
        if steps_data:
            with st.expander("Processing steps"):
                render_steps(steps_data)

    elif status == "error":
        err_msg    = str(job.get("msg", "Unknown error"))
        err_detail = str(job.get("error_detail", ""))
        steps_data = job.get("steps", [])
        ff_logs    = job.get("ffmpeg_logs", [])

        st.markdown(
            f'<div class="error-summary">❌ {err_msg}</div>',
            unsafe_allow_html=True,
        )

        with st.expander("🔍 Show processing steps (click to debug)", expanded=True):
            render_steps(steps_data)

        if ff_logs:
            with st.expander("📋 Full FFmpeg output"):
                render_ffmpeg_logs(ff_logs)

        if err_detail:
            with st.expander("🐍 Python traceback"):
                st.code(err_detail, language="python")

    elif status == "processing":
        pct_val = job.get("pct", 0)
        msg_val = str(job.get("msg", "Processing…"))
        st.progress(pct_val / 100, text=msg_val)

    # ── merge button ────────────────────────────────────────────────────
    can_merge = bool(has_video and has_srt and not is_run)

    btn_lbl = (
        "🔄 Retry"       if is_err  else
        "🔄 Re-merge"    if is_done else
        "▶ Merge This Episode"
    )

    if st.button(
        btn_lbl,
        key=f"merge_ep_{i}",
        disabled=not can_merge,
        use_container_width=True,
    ):
        ep_name = st.session_state.eps[i].get("name") or f"Episode_{i+1}"
        v_bytes = st.session_state.ep_video_bytes.get(i)
        s_bytes = st.session_state.ep_srt_bytes.get(i)
        v_name  = st.session_state.ep_video_names.get(i, "video.mp4")
        s_name  = st.session_state.ep_srt_names.get(i, "subtitle.srt")

        if not v_bytes or not s_bytes:
            st.error("Missing video or subtitle data — please re-upload.")
        else:
            proceed = True
            if check_match(v_name, s_name) == "mismatch":
                proceed = st.checkbox(
                    "⚠️ Mismatch detected — tick to proceed anyway",
                    key=f"mismatch_ok_{i}",
                )

            if proceed:
                st.session_state.eps[i]["job"] = {
                    "status": "processing",
                    "pct": 1,
                    "msg": "Starting…",
                    "path": None,
                }

                prog_bar = st.progress(0, text="Initialising…")

                def make_cb(pb):
                    def cb(pct, msg):
                        try:
                            pb.progress(min(int(pct), 100) / 100, text=str(msg)[:120])
                        except Exception:
                            pass
                    return cb

                os.makedirs("/tmp/merged_videos", exist_ok=True)

                result = process_episode_from_bytes(
                    v_bytes, v_name,
                    s_bytes, s_name,
                    ep_name, mode, "/tmp/merged_videos",
                    progress_cb=make_cb(prog_bar),
                )

                if result["success"]:
                    st.session_state.eps[i]["job"] = {
                        "status":      "completed",
                        "pct":         100,
                        "msg":         result.get("size_str", "Done"),
                        "path":        result.get("path", ""),
                        "size_mb":     result.get("size_mb", 0),
                        "size_str":    result.get("size_str", ""),
                        "steps":       result.get("steps", []),
                        "ffmpeg_logs": result.get("ffmpeg_logs", []),
                    }
                else:
                    st.session_state.eps[i]["job"] = {
                        "status":       "error",
                        "pct":          0,
                        "msg":          result.get("error", "Failed"),
                        "error_detail": result.get("error_detail", ""),
                        "steps":        result.get("steps", []),
                        "ffmpeg_logs":  result.get("ffmpeg_logs", []),
                        "path":         "",
                    }
                st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

# handle deletions
if eps_to_delete:
    for idx in sorted(eps_to_delete, reverse=True):
        if 0 <= idx < len(st.session_state.eps):
            st.session_state.eps.pop(idx)
            for d in [
                st.session_state.ep_video_bytes,
                st.session_state.ep_video_names,
                st.session_state.ep_srt_bytes,
                st.session_state.ep_srt_names,
            ]:
                d.pop(idx, None)
    st.rerun()

if not st.session_state.eps:
    st.markdown(
        '<div style="text-align:center;padding:28px;'
        'color:rgba(255,255,255,.25);font-size:13px">'
        'Click + Add Episode to get started'
        '</div>',
        unsafe_allow_html=True,
    )

st.markdown("</div>", unsafe_allow_html=True)

# ── batch merge ────────────────────────────────────────────────────────────────
st.markdown('<div class="merger-card">', unsafe_allow_html=True)
st.markdown(
    '<div class="batch-divider">─── batch merge all at once ───</div>',
    unsafe_allow_html=True,
)

valid_idx = [
    i for i in range(len(st.session_state.eps))
    if st.session_state.ep_video_bytes.get(i)
    and st.session_state.ep_srt_bytes.get(i)
]
n_valid = len(valid_idx)
batch_lbl = (
    f"▶▶ Merge All {n_valid} Episode{'s' if n_valid != 1 else ''}"
    if n_valid > 0
    else "Merge All Episodes"
)

if st.button(
    batch_lbl, disabled=n_valid == 0,
    use_container_width=True, type="primary"
):
    os.makedirs("/tmp/merged_videos", exist_ok=True)
    overall  = st.progress(0, text="Starting batch…")
    ok_cnt   = 0
    fail_cnt = 0

    for step, i in enumerate(valid_idx):
        ep      = st.session_state.eps[i]
        ep_name = ep.get("name") or f"Episode_{i+1}"
        v_bytes = st.session_state.ep_video_bytes[i]
        s_bytes = st.session_state.ep_srt_bytes[i]
        v_name  = st.session_state.ep_video_names.get(i, "video.mp4")
        s_name  = st.session_state.ep_srt_names.get(i, "subtitle.srt")

        overall.progress(
            step / n_valid,
            text=f"Episode {step+1} of {n_valid}: {ep_name}…"
        )
        holder = st.empty()

        def make_batch_cb(h, name):
            def cb(pct, msg):
                try:
                    h.progress(min(int(pct), 100) / 100, text=f"{name}: {msg}")
                except Exception:
                    pass
            return cb

        result = process_episode_from_bytes(
            v_bytes, v_name,
            s_bytes, s_name,
            ep_name, mode, "/tmp/merged_videos",
            progress_cb=make_batch_cb(holder, ep_name),
        )

        if result["success"]:
            ok_cnt += 1
            holder.success(f"✓ {ep_name} — {result.get('size_str','')}")
            st.session_state.eps[i]["job"] = {
                "status":      "completed",
                "pct":         100,
                "msg":         result.get("size_str", "Done"),
                "path":        result.get("path", ""),
                "size_mb":     result.get("size_mb", 0),
                "size_str":    result.get("size_str", ""),
                "steps":       result.get("steps", []),
                "ffmpeg_logs": result.get("ffmpeg_logs", []),
            }
        else:
            fail_cnt += 1
            holder.error(f"✗ {ep_name} — {result.get('error','Failed')}")
            st.session_state.eps[i]["job"] = {
                "status":       "error",
                "pct":          0,
                "msg":          result.get("error", "Failed"),
                "error_detail": result.get("error_detail", ""),
                "steps":        result.get("steps", []),
                "ffmpeg_logs":  result.get("ffmpeg_logs", []),
                "path":         "",
            }

    overall.progress(
        1.0,
        text=f"Done — {ok_cnt} succeeded, {fail_cnt} failed"
    )
    st.markdown(
        f'<div class="sum-box">'
        f'Batch complete: <strong>{ok_cnt}</strong> completed, '
        f'<strong>{fail_cnt}</strong> failed'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.rerun()

st.markdown("</div>", unsafe_allow_html=True)

# ── download all completed ─────────────────────────────────────────────────────
done_eps = [
    ep for ep in st.session_state.eps
    if isinstance(ep, dict)
    and isinstance(ep.get("job"), dict)
    and ep["job"].get("status") == "completed"
    and ep["job"].get("path")
    and os.path.isfile(str(ep["job"]["path"]))
]

if done_eps:
    st.markdown(
        '<div class="merger-card">'
        '<div class="merger-title">⬇ Download All Completed Files</div>',
        unsafe_allow_html=True,
    )
    for ep in done_eps:
        path  = ep["job"]["path"]
        fname = os.path.basename(path)
        try:
            file_size = os.path.getsize(path)
            ep_name   = ep.get("name") or fname
            with open(path, "rb") as fh:
                st.download_button(
                    label=f"⬇ {ep_name} — {format_file_size(file_size)}",
                    data=fh,
                    file_name=fname,
                    mime="video/mp4",
                    key=f"dlall_{fname}_{uuid.uuid4().hex[:6]}",
                    use_container_width=True,
                )
        except Exception as ex:
            st.warning(f"Could not prepare {fname}: {ex}")
    st.markdown("</div>", unsafe_allow_html=True)

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
    # ── NEW: cache output file bytes so download survives reruns ──
    if "ep_output_bytes" not in st.session_state:
        st.session_state.ep_output_bytes = {}
    if "ep_output_names" not in st.session_state:
        st.session_state.ep_output_names = {}
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
    ".merger-card{background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);border-radius:16px;padding:20px;margin-bottom:16px}"
    ".merger-title{font-size:14px;font-weight:700;color:rgba(255,255,255,.85);margin-bottom:12px}"
    ".hw-chip{display:inline-block;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:700;margin:2px}"
    ".hw-on{background:rgba(16,185,129,.2);border:1px solid rgba(16,185,129,.4);color:#6ee7b7}"
    ".hw-off{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);color:rgba(255,255,255,.3)}"
    ".ep-card{background:rgba(255,255,255,.03);border:1.5px solid rgba(255,255,255,.08);border-radius:12px;padding:16px;margin-bottom:10px}"
    ".ep-done{border-color:rgba(16,185,129,.5)!important;background:rgba(16,185,129,.06)!important}"
    ".ep-error{border-color:rgba(255,59,48,.5)!important;background:rgba(255,59,48,.06)!important}"
    ".ep-running{border-color:rgba(99,102,241,.5)!important;background:rgba(99,102,241,.06)!important}"
    ".badge-ok{display:inline-block;padding:2px 8px;border-radius:20px;font-size:10px;font-weight:700;background:rgba(16,185,129,.15);border:1px solid rgba(16,185,129,.3);color:#6ee7b7}"
    ".mismatch-box{background:rgba(251,191,36,.12);border:1px solid rgba(251,191,36,.35);border-radius:8px;padding:8px 12px;color:#fde68a;font-size:12px;margin-top:6px;line-height:1.6}"
    ".info-box{background:rgba(99,102,241,.1);border:1px solid rgba(99,102,241,.25);border-radius:8px;padding:10px 14px;color:#a5b4fc;font-size:12px;margin-top:8px}"
    ".warn-box{background:rgba(255,59,48,.1);border:1px solid rgba(255,59,48,.3);border-radius:10px;padding:12px 16px;color:#ff6b6b;font-size:12px;margin-bottom:12px}"
    ".how-box{background:rgba(16,185,129,.07);border:1px solid rgba(16,185,129,.25);border-radius:12px;padding:14px 18px;color:#a7f3d0;font-size:13px;margin-bottom:16px;line-height:2}"
    ".dl-box{background:rgba(16,185,129,.1);border:2px solid rgba(16,185,129,.4);border-radius:10px;padding:12px 16px;color:#6ee7b7;font-size:12px;margin-top:8px;word-break:break-all}"
    ".batch-divider{text-align:center;color:rgba(255,255,255,.2);font-size:11px;margin:4px 0 14px}"
    ".sum-box{background:rgba(99,102,241,.08);border:1px solid rgba(99,102,241,.2);border-radius:10px;padding:12px 16px;font-size:12px;color:#a5b4fc;margin-top:10px;line-height:1.8}"
    ".upload-info{background:rgba(99,102,241,.08);border:1px solid rgba(99,102,241,.2);border-radius:8px;padding:6px 10px;color:#a5b4fc;font-size:11px;margin-top:4px}"
    ".error-summary{background:rgba(255,59,48,.12);border:1px solid rgba(255,59,48,.4);border-radius:8px;padding:10px 14px;color:#ff6b6b;font-size:12px;margin-top:6px;font-weight:600}"
    ".log-step{background:rgba(0,0,0,.35);border-left:3px solid rgba(99,102,241,.6);border-radius:0 6px 6px 0;padding:6px 10px;margin:3px 0;color:#c7d2fe;font-size:11px;font-family:monospace}"
    ".log-step-ok{border-left-color:rgba(16,185,129,.6)!important;color:#a7f3d0!important}"
    ".log-step-err{border-left-color:rgba(255,59,48,.7)!important;color:#fca5a5!important}"
    ".ffmpeg-out{background:rgba(0,0,0,.5);border:1px solid rgba(255,255,255,.08);border-radius:6px;padding:10px;color:#e5e7eb;font-size:10px;font-family:monospace;white-space:pre-wrap;max-height:400px;overflow-y:auto;margin-top:6px}"
    ".diag-box{background:rgba(0,0,0,.4);border:1px solid rgba(251,191,36,.3);border-radius:10px;padding:14px;margin-bottom:16px;font-size:12px;font-family:monospace;color:#fde68a}"
    "</style>"
)
st.markdown(CSS, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
#  DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════════
def run_diagnostics():
    results = {}
    try:
        r = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True, timeout=10)
        results["ffmpeg_found"] = r.returncode == 0
        results["ffmpeg_ver"]   = r.stdout.splitlines()[0] if r.stdout else r.stderr[:100]
    except FileNotFoundError:
        results["ffmpeg_found"] = False
        results["ffmpeg_ver"]   = "NOT FOUND — add ffmpeg to packages.txt"
    except Exception as ex:
        results["ffmpeg_found"] = False
        results["ffmpeg_ver"]   = str(ex)

    try:
        r = subprocess.run(["ffprobe", "-version"], capture_output=True, text=True, timeout=10)
        results["ffprobe_found"] = r.returncode == 0
        results["ffprobe_ver"]   = r.stdout.splitlines()[0] if r.stdout else r.stderr[:100]
    except FileNotFoundError:
        results["ffprobe_found"] = False
        results["ffprobe_ver"]   = "NOT FOUND"
    except Exception as ex:
        results["ffprobe_found"] = False
        results["ffprobe_ver"]   = str(ex)

    try:
        tp = tempfile.mkdtemp(prefix="diag_")
        tf = os.path.join(tp, "test.txt")
        with open(tf, "w") as f:
            f.write("ok")
        results["tmp_writable"] = os.path.exists(tf)
        shutil.rmtree(tp, ignore_errors=True)
    except Exception as ex:
        results["tmp_writable"] = False
        results["tmp_err"]      = str(ex)

    try:
        r = subprocess.run(["ffmpeg", "-filters"], capture_output=True, text=True, timeout=10)
        results["has_ass"]       = "ass" in r.stdout
        results["has_subtitles"] = "subtitles" in r.stdout
    except Exception:
        results["has_ass"]       = False
        results["has_subtitles"] = False

    try:
        r = subprocess.run(["ffmpeg", "-encoders"], capture_output=True, text=True, timeout=10)
        out = r.stdout
        results["enc_libx264"]      = "libx264" in out
        results["enc_nvenc"]        = "h264_nvenc" in out
        results["enc_videotoolbox"] = "h264_videotoolbox" in out
        results["enc_qsv"]          = "h264_qsv" in out
        results["enc_vaapi"]        = "h264_vaapi" in out
    except Exception:
        results["enc_libx264"] = False

    results["functional_test"] = "not run"
    results["functional_err"]  = ""
    if results.get("ffmpeg_found") and results.get("tmp_writable"):
        try:
            td  = tempfile.mkdtemp(prefix="functest_")
            out = os.path.join(td, "out.mp4")
            srt = os.path.join(td, "test.srt")
            with open(srt, "w") as f:
                f.write("1\n00:00:00,000 --> 00:00:01,000\nTest subtitle\n\n")
            srt_esc = srt.replace("\\", "\\\\").replace(":", "\\:")
            cmd = [
                "ffmpeg", "-y",
                "-f", "lavfi", "-i", "color=black:size=320x240:duration=1:rate=25",
                "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
                "-t", "1",
                "-vf", f"subtitles='{srt_esc}'",
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30",
                "-c:a", "aac", "-shortest", out
            ]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if r.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 1000:
                results["functional_test"] = "PASS"
            else:
                results["functional_test"] = "FAIL"
                results["functional_err"]  = (
                    f"RC={r.returncode}\nSTDOUT:\n{r.stdout[-400:]}\nSTDERR:\n{r.stderr[-800:]}"
                )
            shutil.rmtree(td, ignore_errors=True)
        except Exception:
            results["functional_test"] = "EXCEPTION"
            results["functional_err"]  = traceback.format_exc()
    return results


@st.cache_data(ttl=120)
def cached_diagnostics():
    return run_diagnostics()


def check_hw_accel():
    hw = {"nvenc": False, "qsv": False, "videotoolbox": False, "vaapi": False}
    try:
        r = subprocess.run(["ffmpeg", "-encoders"], capture_output=True, text=True, timeout=10)
        out = r.stdout
        if "h264_nvenc"        in out: hw["nvenc"]        = True
        if "h264_qsv"          in out: hw["qsv"]          = True
        if "h264_videotoolbox" in out: hw["videotoolbox"] = True
        if "h264_vaapi"        in out: hw["vaapi"]        = True
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
        "-preset", "ultrafast", "-crf", "26",
        "-threads", str(FF_THREADS), "-tune", "fastdecode"
    ]


def format_file_size(n):
    if n < 1024:     return f"{n} B"
    if n < 1024**2:  return f"{round(n/1024,1)} KB"
    if n < 1024**3:  return f"{round(n/1024**2,1)} MB"
    return f"{round(n/1024**3,2)} GB"


def save_bytes_to_file(data, path):
    with open(path, "wb") as f:
        f.write(data)
    return len(data)


def unique_dest(folder, filename):
    dest = os.path.join(folder, filename)
    if not os.path.exists(dest):
        return dest
    stem, suf = os.path.splitext(filename)
    return os.path.join(folder, stem + "_" + uuid.uuid4().hex[:4] + suf)


def file_ok(path, min_bytes=10000):
    return os.path.exists(path) and os.path.getsize(path) >= min_bytes


def parse_srt(path):
    content = None
    for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
        try:
            with open(path, "r", encoding=enc) as f:
                content = f.read()
            break
        except Exception:
            continue
    if not content:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    content = (content.replace("\ufeff", "")
                      .replace("\r\n", "\n")
                      .replace("\r", "\n"))
    entries = []
    for block in re.split(r"\n\s*\n", content.strip()):
        lines = block.strip().split("\n")
        if len(lines) < 2:
            continue
        time_match = time_idx = None
        for li, line in enumerate(lines):
            m = re.match(
                r"(\d{2}):(\d{2}):(\d{2})[,.](\d{3})\s*-->\s*"
                r"(\d{2}):(\d{2}):(\d{2})[,.](\d{3})",
                line.strip()
            )
            if m:
                time_match, time_idx = m, li
                break
        if not time_match:
            continue
        g     = time_match.groups()
        start = int(g[0])*3600 + int(g[1])*60 + int(g[2]) + int(g[3])/1000
        end   = int(g[4])*3600 + int(g[5])*60 + int(g[6]) + int(g[7])/1000
        text  = re.sub(r"<[^>]+>", "", "\n".join(lines[time_idx+1:]))
        text  = re.sub(r"\{[^}]+\}", "", text).strip()
        if text:
            entries.append({"start": start, "end": end, "text": text})
    return entries


def fmt_srt_time(s):
    h, m, sc, ms = int(s//3600), int((s%3600)//60), int(s%60), int((s%1)*1000)
    return f"{h:02d}:{m:02d}:{sc:02d},{ms:03d}"

def fmt_ass_time(s):
    h, m, sc, cs = int(s//3600), int((s%3600)//60), int(s%60), int((s%1)*100)
    return f"{h}:{m:02d}:{sc:02d}.{cs:02d}"

def clean_srt(src, dst):
    entries = parse_srt(src)
    with open(dst, "w", encoding="utf-8") as f:
        for i, e in enumerate(entries, 1):
            f.write(f"{i}\n{fmt_srt_time(e['start'])} --> {fmt_srt_time(e['end'])}\n{e['text']}\n\n")
    return len(entries)

def create_ass(srt_path, ass_path, w=1920, h=1080):
    entries = parse_srt(srt_path)
    if not entries:
        return 0
    fs, mv, mlr = max(int(h*.045), 24), int(h*.06), int(w*.05)
    hdr = (
        "[Script Info]\nScriptType: v4.00+\n"
        f"PlayResX: {w}\nPlayResY: {h}\nWrapStyle: 0\nScaledBorderAndShadow: yes\n\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
        "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
        "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding\n"
        f"Style: Default,Arial,{fs},&H00FFFFFF,&H000000FF,&H00000000,&H96000000,"
        f"0,0,0,0,100,100,0,0,1,2,1,2,{mlr},{mlr},{mv},1\n\n"
        "[Events]\nFormat: Layer, Start, End, Style, Name, "
        "MarginL, MarginR, MarginV, Effect, Text\n"
    )
    with open(ass_path, "w", encoding="utf-8") as f:
        f.write(hdr)
        for e in entries:
            txt = e["text"].replace("\n","\\N").replace("{","").replace("}","")
            f.write(
                f"Dialogue: 0,{fmt_ass_time(e['start'])},{fmt_ass_time(e['end'])}"
                f",Default,,0,0,0,,{txt}\n"
            )
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
                    w   = int(s.get("width",  1920))
                    h   = int(s.get("height", 1080))
                    dur = float(s.get("duration", 0))
            if dur == 0:
                dur = float(data.get("format", {}).get("duration", 0))
            return {"width": w, "height": h, "duration": dur}
        return {"width": 1920, "height": 1080, "duration": 0,
                "probe_error": r.stderr[:400]}
    except Exception as ex:
        return {"width": 1920, "height": 1080, "duration": 0,
                "probe_error": str(ex)}

def safe_get_job(ep):
    if not isinstance(ep, dict): return {}
    j = ep.get("job")
    return j if isinstance(j, dict) else {}


def run_ffmpeg_logged(cmd, duration, progress_cb, start_pct, end_pct):
    full_stderr = []
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1
        )
        for line in proc.stderr:
            full_stderr.append(line)
            if duration and duration > 0:
                m = re.search(r"time=(\d+):(\d+):([\d.]+)", line)
                if m:
                    elapsed = (int(m.group(1))*3600 +
                               int(m.group(2))*60 +
                               float(m.group(3)))
                    ratio = min(elapsed / duration, 1.0)
                    pct   = int(start_pct + ratio*(end_pct - start_pct))
                    if progress_cb:
                        progress_cb(pct, f"Encoding… {int(ratio*100)}%")
        proc.wait(timeout=86400)
        return proc.returncode, "".join(full_stderr)
    except subprocess.TimeoutExpired:
        try: proc.kill()
        except Exception: pass
        return -1, "FFmpeg timed out"
    except Exception:
        return -1, traceback.format_exc()


def process_episode_from_bytes(
    video_bytes, video_name,
    srt_bytes,   srt_name,
    ep_name, merge_type, dl_folder,
    progress_cb=None
):
    work_dir = None
    out_file = None
    steps    = []
    ff_logs  = []

    def upd(pct, msg):
        if progress_cb:
            try: progress_cb(pct, msg)
            except Exception: pass

    def step(label, ok, detail=""):
        steps.append({"label": label, "ok": ok, "detail": str(detail)})

    def extract_errors(stderr):
        lines = stderr.splitlines()
        bad = [l for l in lines if any(
            k in l.lower() for k in
            ["error","invalid","failed","no such","unable","cannot",
             "could not","not found","permission","codec","filter"]
        )]
        return "\n".join(bad[-12:]) if bad else "\n".join(lines[-20:])

    try:
        upd(1, "Preparing workspace…")
        work_dir = tempfile.mkdtemp(prefix="ep_")
        step("Temp dir created", True, work_dir)

        v_ext  = Path(video_name).suffix.lower() or ".mp4"
        s_ext  = Path(srt_name).suffix.lower()   or ".srt"
        v_path = os.path.join(work_dir, "video"    + v_ext)
        s_path = os.path.join(work_dir, "subtitle" + s_ext)

        upd(2, "Writing video…")
        save_bytes_to_file(video_bytes, v_path)
        v_size = os.path.getsize(v_path)
        step(f"Video written ({format_file_size(v_size)})", v_size >= 1000,
             v_path if v_size >= 1000 else f"TOO SMALL: {v_size} bytes")
        if v_size < 1000:
            raise ValueError(f"Video only {v_size} bytes — corrupt/incomplete")

        upd(6, "Writing subtitle…")
        save_bytes_to_file(srt_bytes, s_path)
        s_size = os.path.getsize(s_path)
        step(f"Subtitle written ({format_file_size(s_size)})", s_size >= 5,
             s_path if s_size >= 5 else f"TOO SMALL: {s_size} bytes")
        if s_size < 5:
            raise ValueError(f"Subtitle only {s_size} bytes — corrupt")

        upd(7, "Parsing subtitles…")
        try:
            entries = parse_srt(s_path)
        except Exception as ex:
            step("SRT parse", False, str(ex))
            raise ValueError(f"Cannot parse subtitle: {ex}")
        step(f"SRT parsed: {len(entries)} entries", len(entries) > 0,
             f"First: {entries[0]['text'][:80]}" if entries else "EMPTY")
        if not entries:
            raise ValueError("No subtitle entries — check timestamps & format")

        upd(9, "Probing video…")
        info     = get_video_info(v_path)
        probe_ok = "probe_error" not in info
        step(f"Video probe: {info['width']}x{info['height']} {info['duration']:.1f}s",
             probe_ok, info.get("probe_error", "OK"))

        safe     = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", ep_name)
        safe     = re.sub(r"_+", "_", safe).strip("_") or "episode"
        encoder, enc_args = get_best_encoder()
        step(f"Encoder: {encoder}", True)
        os.makedirs(dl_folder, exist_ok=True)

        # ══════════════════════════════════════════════════
        #  HARD SUB
        # ══════════════════════════════════════════════════
        if merge_type == "hard":
            out_file = unique_dest(dl_folder, safe + ".mp4")
            success  = False

            def try_hard(label, cmd, s_pct, e_pct):
                nonlocal success
                if success:
                    return
                step(f"Trying {label}", True, " ".join(cmd))
                upd(s_pct, f"FFmpeg: {label}…")
                rc, stderr = run_ffmpeg_logged(cmd, info["duration"], upd, s_pct, e_pct)
                ff_logs.append(
                    f"=== {label} ===\nCMD: {' '.join(cmd)}\nRC: {rc}\n\n{stderr}"
                )
                if file_ok(out_file):
                    success = True
                    step(f"{label} → SUCCESS ✓", True,
                         f"Output: {format_file_size(os.path.getsize(out_file))}")
                else:
                    step(f"{label} → FAILED (rc={rc})", False, extract_errors(stderr))
                    if os.path.exists(out_file):
                        os.remove(out_file)

            # Method A — ASS
            try:
                ass_path = os.path.join(work_dir, "styled.ass")
                n_ass    = create_ass(s_path, ass_path, info["width"], info["height"])
                step(f"ASS file: {n_ass} entries", n_ass > 0)
                if n_ass > 0:
                    ass_esc = (os.path.abspath(ass_path)
                               .replace("\\", "\\\\").replace(":", "\\:"))
                    try_hard(
                        "Method-A: ass filter",
                        ["ffmpeg","-y","-i",os.path.abspath(v_path),
                         "-vf", f"ass={ass_esc}",
                         "-c:v", encoder] + enc_args +
                        ["-c:a","copy","-movflags","+faststart",
                         os.path.abspath(out_file)],
                        12, 88
                    )
            except Exception as ex:
                step("Method-A exception", False, traceback.format_exc())

            # Method B — subtitles filter
            if not success:
                try:
                    cs = os.path.join(work_dir, "clean.srt")
                    clean_srt(s_path, cs)
                    cs_esc = (os.path.abspath(cs)
                              .replace("\\", "\\\\").replace(":", "\\:"))
                    try_hard(
                        "Method-B: subtitles filter",
                        ["ffmpeg","-y","-i",os.path.abspath(v_path),
                         "-vf", f"subtitles={cs_esc}",
                         "-c:v", encoder] + enc_args +
                        ["-c:a","copy","-movflags","+faststart",
                         os.path.abspath(out_file)],
                        15, 88
                    )
                except Exception:
                    step("Method-B exception", False, traceback.format_exc())

            # Method C — short path in /tmp
            if not success:
                try:
                    simple = f"/tmp/sub_{uuid.uuid4().hex[:6]}.srt"
                    cs_src = os.path.join(work_dir, "clean.srt")
                    shutil.copy2(cs_src if os.path.exists(cs_src) else s_path, simple)
                    try_hard(
                        "Method-C: short-path subtitles",
                        ["ffmpeg","-y","-i",os.path.abspath(v_path),
                         "-vf", f"subtitles={simple}",
                         "-c:v", encoder] + enc_args +
                        ["-c:a","copy","-movflags","+faststart",
                         os.path.abspath(out_file)],
                        18, 88
                    )
                    try: os.remove(simple)
                    except Exception: pass
                except Exception:
                    step("Method-C exception", False, traceback.format_exc())

            # Method D — re-encode without subtitle filter as last resort
            if not success:
                try:
                    cs_src = os.path.join(work_dir, "clean.srt")
                    if not os.path.exists(cs_src):
                        clean_srt(s_path, cs_src)
                    try_hard(
                        "Method-D: srt input stream",
                        ["ffmpeg","-y",
                         "-i", os.path.abspath(v_path),
                         "-i", cs_src,
                         "-map","0:v","-map","0:a?",
                         "-vf", "subtitles=subtitle.srt",
                         "-c:v", encoder] + enc_args +
                        ["-c:a","copy","-movflags","+faststart",
                         os.path.abspath(out_file)],
                        21, 88
                    )
                except Exception:
                    step("Method-D exception", False, traceback.format_exc())

            if not success:
                raise RuntimeError(
                    "All 4 hard-sub methods failed — check FFmpeg logs below"
                )

        # ══════════════════════════════════════════════════
        #  SOFT SUB
        # ══════════════════════════════════════════════════
        else:
            success = False
            upd(15, "Soft sub — stream copy…")

            out_mkv = unique_dest(dl_folder, safe + ".mkv")
            try:
                sub_codec = "ass" if s_ext in (".ass", ".ssa") else "srt"
                cmd = [
                    "ffmpeg","-y",
                    "-i", os.path.abspath(v_path),
                    "-i", os.path.abspath(s_path),
                    "-map","0:v","-map","0:a?","-map","1:0",
                    "-c:v","copy","-c:a","copy","-c:s", sub_codec,
                    "-metadata:s:s:0","language=eng",
                    "-disposition:s:0","default",
                    out_mkv
                ]
                step("Soft Method-1: MKV stream copy", True, " ".join(cmd))
                upd(20, "FFmpeg: MKV soft sub…")
                rc, stderr = run_ffmpeg_logged(cmd, info["duration"], upd, 20, 88)
                ff_logs.append(f"=== Soft-MKV ===\nCMD: {' '.join(cmd)}\nRC: {rc}\n\n{stderr}")
                if file_ok(out_mkv):
                    out_file = out_mkv
                    success  = True
                    step("Soft Method-1 (MKV) SUCCESS ✓", True,
                         format_file_size(os.path.getsize(out_mkv)))
                else:
                    step("Soft Method-1 (MKV) FAILED", False, extract_errors(stderr))
                    if os.path.exists(out_mkv): os.remove(out_mkv)
            except Exception:
                step("Soft Method-1 exception", False, traceback.format_exc())
                if os.path.exists(out_mkv):
                    try: os.remove(out_mkv)
                    except Exception: pass

            if not success:
                out_mp4 = unique_dest(dl_folder, safe + ".mp4")
                try:
                    cs = os.path.join(work_dir, "clean.srt")
                    clean_srt(s_path, cs)
                    cmd = [
                        "ffmpeg","-y",
                        "-i", os.path.abspath(v_path),
                        "-i", cs,
                        "-map","0:v","-map","0:a?","-map","1:0",
                        "-c:v","copy","-c:a","copy","-c:s","mov_text",
                        "-metadata:s:s:0","language=eng",
                        "-disposition:s:0","default",
                        "-movflags","+faststart",
                        os.path.abspath(out_mp4)
                    ]
                    step("Soft Method-2: MP4 mov_text", True, " ".join(cmd))
                    upd(45, "FFmpeg: MP4 soft sub…")
                    rc, stderr = run_ffmpeg_logged(cmd, info["duration"], upd, 45, 88)
                    ff_logs.append(
                        f"=== Soft-MP4 ===\nCMD: {' '.join(cmd)}\nRC: {rc}\n\n{stderr}"
                    )
                    if file_ok(out_mp4):
                        out_file = out_mp4
                        success  = True
                        step("Soft Method-2 (MP4) SUCCESS ✓", True,
                             format_file_size(os.path.getsize(out_mp4)))
                    else:
                        step("Soft Method-2 (MP4) FAILED", False, extract_errors(stderr))
                        if os.path.exists(out_mp4): os.remove(out_mp4)
                except Exception:
                    step("Soft Method-2 exception", False, traceback.format_exc())

            if not success:
                raise RuntimeError("Both soft-sub methods failed — check FFmpeg logs")

        # ── success ────────────────────────────────────────
        sz = os.path.getsize(out_file)
        ss = format_file_size(sz)
        upd(100, f"Done! {ss}")
        step(f"Output: {os.path.basename(out_file)} ({ss})", True)

        # ── READ OUTPUT INTO MEMORY so download survives reruns ──
        with open(out_file, "rb") as fh:
            output_bytes = fh.read()

        return {
            "success":      True,
            "path":         out_file,
            "output_bytes": output_bytes,       # <── KEY FIX
            "size_mb":      round(sz/1024/1024, 1),
            "size_str":     ss,
            "filename":     os.path.basename(out_file),
            "steps":        steps,
            "ffmpeg_logs":  ff_logs,
        }

    except Exception as exc:
        tb = traceback.format_exc()
        step(f"FATAL: {exc}", False, tb)
        upd(0, f"Failed: {str(exc)[:120]}")
        if out_file and os.path.exists(out_file):
            try: os.remove(out_file)
            except Exception: pass
        return {
            "success":      False,
            "error":        str(exc),
            "error_detail": tb,
            "steps":        steps,
            "ffmpeg_logs":  ff_logs,
        }
    finally:
        if work_dir and os.path.isdir(work_dir):
            shutil.rmtree(work_dir, ignore_errors=True)


def extract_number(filename):
    stem = Path(filename).stem
    for pat in [r"[Ee][Pp]?(\d+)", r"[Ee](\d+)"]:
        m = re.search(pat, stem)
        if m: return int(m.group(1))
    nums = re.findall(r"\d+", stem)
    return int(nums[-1]) if nums else None

def check_match(vn, sn):
    v, s = extract_number(vn), extract_number(sn)
    if v is None and s is None: return "unknown"
    if v is None or  s is None: return "unknown"
    return "ok" if v == s else "mismatch"

def render_steps(steps):
    for s in steps:
        cls  = "log-step-ok" if s["ok"] else "log-step-err"
        icon = "✓" if s["ok"] else "✗"
        det  = (f'<br><span style="opacity:.75;white-space:pre-wrap">{s["detail"]}</span>'
                if s["detail"] else "")
        st.markdown(
            f'<div class="log-step {cls}">{icon} {s["label"]}{det}</div>',
            unsafe_allow_html=True
        )

def render_ff_logs(logs):
    if not logs: return
    st.markdown(
        f'<div class="ffmpeg-out">{"<br><br>".join(logs)}</div>',
        unsafe_allow_html=True
    )


# ═══════════════════════════════════════════════════════════════════
#  PAGE HEADER
# ═══════════════════════════════════════════════════════════════════
diag     = cached_diagnostics()
hw       = check_hw_accel()
enc_name, _ = get_best_encoder()

chips_html = "".join(
    f'<span class="hw-chip {"hw-on" if v else "hw-off"}">'
    f'{"⚡" if v else "○"} {k.upper()}</span>'
    for k, v in hw.items()
)

st.markdown(
    '<div style="text-align:center;padding:24px 20px 16px;'
    'background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);'
    'border-radius:20px;margin-bottom:20px">'
    '<div style="font-size:46px;margin-bottom:8px">🎬</div>'
    '<h1 style="font-size:24px;font-weight:800;margin:0 0 4px">'
    'Video &amp; Subtitle Merger</h1>'
    '<p style="color:rgba(255,255,255,.5);font-size:13px;margin:0 0 6px">'
    f'Up to {MAX_EPISODES} episodes &nbsp;•&nbsp; Files up to '
    '<strong style="color:#6ee7b7">10 GB</strong></p>'
    f'<div style="margin-top:8px">{chips_html}</div>'
    '</div>',
    unsafe_allow_html=True,
)

with st.expander(
    "🔧 System Diagnostics — "
    + ("✅ all OK" if diag.get("functional_test") == "PASS" else "⚠️ CHECK REQUIRED"),
    expanded=(diag.get("functional_test") != "PASS")
):
    ok_  = "✅"
    bad_ = "❌"
    st.markdown(f"""
| Check | Result |
|---|---|
| FFmpeg | {ok_ if diag.get('ffmpeg_found') else bad_} `{diag.get('ffmpeg_ver','')}` |
| FFprobe | {ok_ if diag.get('ffprobe_found') else bad_} `{diag.get('ffprobe_ver','')}` |
| /tmp writable | {ok_ if diag.get('tmp_writable') else bad_} |
| `ass` filter | {ok_ if diag.get('has_ass') else bad_} |
| `subtitles` filter | {ok_ if diag.get('has_subtitles') else bad_} |
| libx264 | {ok_ if diag.get('enc_libx264') else bad_} |
| Functional test | {ok_ if diag.get('functional_test')=='PASS' else bad_} `{diag.get('functional_test','')}` |
    """)
    if diag.get("functional_test") != "PASS" and diag.get("functional_err"):
        st.markdown("**Functional test error:**")
        st.code(diag["functional_err"], language="bash")
    if st.button("🔄 Re-run diagnostics"):
        cached_diagnostics.clear()
        st.rerun()

# ── mode ────────────────────────────────────────────────────────────
st.markdown(
    '<div class="merger-card"><div class="merger-title">Subtitle Mode</div>',
    unsafe_allow_html=True,
)
mode = st.radio(
    "mode", options=["hard", "soft"],
    format_func=lambda x: (
        "Hard — Burned-in subtitles (re-encodes)" if x == "hard"
        else "Soft — Selectable track, stream copy (FASTEST)"
    ),
    label_visibility="collapsed", horizontal=True,
)
if mode == "hard":
    st.markdown(
        f'<div class="info-box">Encoder: <strong>{enc_name}</strong>'
        f' · {FF_THREADS} threads</div>',
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        '<div class="info-box">Stream copy — 10 GB finishes in seconds!</div>',
        unsafe_allow_html=True,
    )
st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
#  EPISODE LIST
# ═══════════════════════════════════════════════════════════════════
st.markdown('<div class="merger-card">', unsafe_allow_html=True)
ep_count = len(st.session_state.eps)
st.markdown(
    f'<div class="merger-title">Episodes '
    f'<span style="background:rgba(99,102,241,.2);color:#a5b4fc;'
    f'font-size:11px;padding:2px 10px;border-radius:20px;font-weight:700">'
    f'{ep_count}/{MAX_EPISODES}</span></div>',
    unsafe_allow_html=True,
)

c1, c2, c3, c4, c5 = st.columns([2, 1, 2, 2, 2])
with c1:
    if st.button("+ Add Episode", use_container_width=True, disabled=ep_count >= MAX_EPISODES):
        st.session_state.eps.append({"name": "", "job": None})
        st.rerun()
with c2:
    n_bulk = st.number_input("N", min_value=1, max_value=MAX_EPISODES,
                              value=5, label_visibility="collapsed")
with c3:
    if st.button(f"Add {int(n_bulk)} Episodes", use_container_width=True,
                 disabled=ep_count >= MAX_EPISODES):
        to_add = min(int(n_bulk), MAX_EPISODES - ep_count)
        for k in range(to_add):
            st.session_state.eps.append({
                "name": f"EP{str(ep_count+k+1).zfill(2)}", "job": None
            })
        st.rerun()
with c4:
    with st.expander("Bulk Rename"):
        bp  = st.text_input("Prefix", value="EP", key="bp")
        bs  = st.number_input("Start", min_value=1, value=1, key="bs")
        bpd = st.number_input("Pad", min_value=1, max_value=4, value=2, key="bpd")
        bsf = st.text_input("Suffix", value="", key="bsf")
        if st.button("Apply", use_container_width=True):
            for k in range(len(st.session_state.eps)):
                st.session_state.eps[k]["name"] = (
                    bp + str(int(bs)+k).zfill(int(bpd)) + bsf
                )
            st.rerun()
with c5:
    if st.button("Clear All", use_container_width=True, disabled=ep_count == 0):
        for key in ["eps","ep_video_bytes","ep_video_names",
                    "ep_srt_bytes","ep_srt_names","debug_logs",
                    "ep_output_bytes","ep_output_names"]:
            st.session_state[key] = {} if key != "eps" else []
        st.rerun()

st.divider()
eps_to_delete = []

for i in range(len(st.session_state.eps)):
    ep = st.session_state.eps[i]
    if not isinstance(ep, dict):
        continue

    job     = safe_get_job(ep)
    status  = job.get("status", "")
    is_done = status == "completed"
    is_err  = status == "error"
    is_run  = status == "processing"

    card_cls = (
        " ep-done"    if is_done else
        " ep-error"   if is_err  else
        " ep-running" if is_run  else ""
    )
    st.markdown(f'<div class="ep-card{card_cls}">', unsafe_allow_html=True)

    h1, h2, h3, h4 = st.columns([4, 1, 1, 1])
    with h1:
        nm = st.text_input(
            f"Name {i+1}",
            value=ep.get("name","") or f"EP{str(i+1).zfill(2)}",
            key=f"epname_{i}", label_visibility="collapsed",
            placeholder=f"EP{str(i+1).zfill(2)}"
        )
        st.session_state.eps[i]["name"] = nm
    with h2:
        if st.button("Up", key=f"up_{i}", disabled=(i == 0)):
            for d in [st.session_state.ep_video_bytes, st.session_state.ep_video_names,
                      st.session_state.ep_srt_bytes,   st.session_state.ep_srt_names,
                      st.session_state.ep_output_bytes, st.session_state.ep_output_names]:
                d[i], d[i-1] = d.get(i-1), d.get(i)
            st.session_state.eps[i], st.session_state.eps[i-1] = (
                st.session_state.eps[i-1], st.session_state.eps[i]
            )
            st.rerun()
    with h3:
        if st.button("Dn", key=f"dn_{i}", disabled=(i == len(st.session_state.eps)-1)):
            for d in [st.session_state.ep_video_bytes, st.session_state.ep_video_names,
                      st.session_state.ep_srt_bytes,   st.session_state.ep_srt_names,
                      st.session_state.ep_output_bytes, st.session_state.ep_output_names]:
                d[i], d[i+1] = d.get(i+1), d.get(i)
            st.session_state.eps[i], st.session_state.eps[i+1] = (
                st.session_state.eps[i+1], st.session_state.eps[i]
            )
            st.rerun()
    with h4:
        if st.button("✕", key=f"del_{i}"):
            eps_to_delete.append(i)

    f1, f2 = st.columns(2)
    with f1:
        vf = st.file_uploader(
            "Video (up to 10 GB)",
            type=["mp4","mkv","avi","mov","wmv","flv","webm","m4v"],
            key=f"vup_{i}"
        )
        if vf is not None:
            vf.seek(0)
            vb = vf.read()
            st.session_state.ep_video_bytes[i] = vb
            st.session_state.ep_video_names[i] = vf.name
            if status in ("completed", "error"):
                st.session_state.eps[i]["job"] = None
                st.session_state.ep_output_bytes.pop(i, None)
            st.markdown(
                f'<div class="upload-info">✓ {vf.name} — {format_file_size(len(vb))}</div>',
                unsafe_allow_html=True
            )
        elif i in st.session_state.ep_video_bytes:
            vb  = st.session_state.ep_video_bytes[i]
            vnm = st.session_state.ep_video_names.get(i, "video")
            st.markdown(
                f'<div class="upload-info">✓ {vnm} — {format_file_size(len(vb))}</div>',
                unsafe_allow_html=True
            )

    with f2:
        sf = st.file_uploader(
            "Subtitle (.srt/.ass)",
            type=["srt","ass","ssa","vtt","sub"],
            key=f"sup_{i}"
        )
        if sf is not None:
            sf.seek(0)
            sb = sf.read()
            st.session_state.ep_srt_bytes[i] = sb
            st.session_state.ep_srt_names[i] = sf.name
            if status in ("completed", "error"):
                st.session_state.eps[i]["job"] = None
                st.session_state.ep_output_bytes.pop(i, None)
            st.markdown(
                f'<div class="upload-info">✓ {sf.name} — {format_file_size(len(sb))}</div>',
                unsafe_allow_html=True
            )
        elif i in st.session_state.ep_srt_bytes:
            sb  = st.session_state.ep_srt_bytes[i]
            snm = st.session_state.ep_srt_names.get(i, "subtitle")
            st.markdown(
                f'<div class="upload-info">✓ {snm} — {format_file_size(len(sb))}</div>',
                unsafe_allow_html=True
            )

    has_v = bool(st.session_state.ep_video_bytes.get(i))
    has_s = bool(st.session_state.ep_srt_bytes.get(i))
    v_nm  = st.session_state.ep_video_names.get(i, "")
    s_nm  = st.session_state.ep_srt_names.get(i, "")

    if has_v and has_s and v_nm and s_nm:
        try:
            mt = check_match(v_nm, s_nm)
            if mt == "mismatch":
                st.markdown(
                    f'<div class="mismatch-box">⚠ Mismatch: '
                    f'Video #{extract_number(v_nm)} vs Sub #{extract_number(s_nm)}'
                    f'<br>{v_nm} ↔ {s_nm}</div>',
                    unsafe_allow_html=True
                )
            elif mt == "ok":
                st.markdown(
                    f'<span class="badge-ok">#{extract_number(v_nm)} matched</span>',
                    unsafe_allow_html=True
                )
        except Exception:
            pass

    # ── job status ──────────────────────────────────────────────────
    job    = safe_get_job(st.session_state.eps[i])
    status = job.get("status", "")

    if status == "completed":
        st.success(f"✅ Done — {job.get('size_str','')}")

        # ── FIXED DOWNLOAD BUTTON ──────────────────────────────────
        # Get bytes from session_state (persists across reruns)
        out_bytes = st.session_state.ep_output_bytes.get(i)
        out_fname = st.session_state.ep_output_names.get(i, "merged_video.mp4")

        if out_bytes:
            st.markdown(
                '<div class="dl-box">✅ File ready — click below to download</div>',
                unsafe_allow_html=True
            )
            st.download_button(
                label=f"⬇ Download {out_fname} ({format_file_size(len(out_bytes))})",
                data=out_bytes,           # bytes already in memory — no file needed
                file_name=out_fname,
                mime="video/mp4",
                key=f"dl_{i}_{uuid.uuid4().hex[:6]}",
                use_container_width=True,
            )
        else:
            # fallback: try reading from disk if still there
            out_path = job.get("path","")
            if out_path and os.path.isfile(out_path):
                with open(out_path, "rb") as fh:
                    fb = fh.read()
                st.session_state.ep_output_bytes[i] = fb
                st.session_state.ep_output_names[i] = os.path.basename(out_path)
                st.download_button(
                    label=f"⬇ Download {os.path.basename(out_path)} "
                          f"({format_file_size(len(fb))})",
                    data=fb,
                    file_name=os.path.basename(out_path),
                    mime="video/mp4",
                    key=f"dl_{i}_{uuid.uuid4().hex[:6]}",
                    use_container_width=True,
                )
            else:
                st.warning("Output file no longer on disk — please re-merge.")

        if job.get("steps"):
            with st.expander("Processing steps"):
                render_steps(job["steps"])

    elif status == "error":
        st.markdown(
            f'<div class="error-summary">❌ {job.get("msg","Error")}</div>',
            unsafe_allow_html=True
        )
        with st.expander("🔍 Processing steps", expanded=True):
            render_steps(job.get("steps", []))
        if job.get("ffmpeg_logs"):
            with st.expander("📋 Full FFmpeg output"):
                render_ff_logs(job["ffmpeg_logs"])
        if job.get("error_detail"):
            with st.expander("🐍 Python traceback"):
                st.code(job["error_detail"], language="python")

    elif status == "processing":
        st.progress(
            job.get("pct", 0) / 100,
            text=job.get("msg", "Processing…")
        )

    # ── merge button ─────────────────────────────────────────────────
    can_merge = bool(has_v and has_s and not is_run)
    btn_lbl   = (
        "🔄 Retry"    if is_err  else
        "🔄 Re-merge" if is_done else
        "▶ Merge This Episode"
    )

    if st.button(btn_lbl, key=f"merge_ep_{i}",
                 disabled=not can_merge, use_container_width=True):
        ep_name = st.session_state.eps[i].get("name") or f"Episode_{i+1}"
        vb      = st.session_state.ep_video_bytes.get(i)
        sb      = st.session_state.ep_srt_bytes.get(i)
        v_name  = st.session_state.ep_video_names.get(i, "video.mp4")
        s_name  = st.session_state.ep_srt_names.get(i, "subtitle.srt")

        if not vb or not sb:
            st.error("Missing files — please re-upload")
        else:
            proceed = True
            if check_match(v_name, s_name) == "mismatch":
                proceed = st.checkbox(
                    "Mismatch — tick to proceed anyway",
                    key=f"mmo_{i}"
                )
            if proceed:
                # clear old output bytes
                st.session_state.ep_output_bytes.pop(i, None)
                st.session_state.ep_output_names.pop(i, None)

                st.session_state.eps[i]["job"] = {
                    "status": "processing", "pct": 1,
                    "msg": "Starting…", "path": None
                }
                pb = st.progress(0, text="Initialising…")

                def make_cb(p):
                    def cb(pct, msg):
                        try:
                            p.progress(min(int(pct), 100) / 100, text=str(msg)[:120])
                        except Exception:
                            pass
                    return cb

                os.makedirs("/tmp/merged_videos", exist_ok=True)
                result = process_episode_from_bytes(
                    vb, v_name, sb, s_name,
                    ep_name, mode, "/tmp/merged_videos",
                    progress_cb=make_cb(pb)
                )

                if result["success"]:
                    # ── store output bytes in session_state ──────────
                    st.session_state.ep_output_bytes[i] = result["output_bytes"]
                    st.session_state.ep_output_names[i] = result["filename"]

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
            for d in [st.session_state.ep_video_bytes,
                      st.session_state.ep_video_names,
                      st.session_state.ep_srt_bytes,
                      st.session_state.ep_srt_names,
                      st.session_state.ep_output_bytes,
                      st.session_state.ep_output_names]:
                d.pop(idx, None)
    st.rerun()

if not st.session_state.eps:
    st.markdown(
        '<div style="text-align:center;padding:28px;'
        'color:rgba(255,255,255,.25);font-size:13px">'
        'Click + Add Episode to get started</div>',
        unsafe_allow_html=True,
    )

st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
#  BATCH MERGE
# ═══════════════════════════════════════════════════════════════════
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
n_valid   = len(valid_idx)
batch_lbl = (
    f"▶▶ Merge All {n_valid} Episode{'s' if n_valid != 1 else ''}"
    if n_valid > 0 else "Merge All Episodes"
)

if st.button(batch_lbl, disabled=n_valid == 0,
             use_container_width=True, type="primary"):
    os.makedirs("/tmp/merged_videos", exist_ok=True)
    overall        = st.progress(0, text="Starting batch…")
    ok_cnt = fail_cnt = 0

    for step_n, i in enumerate(valid_idx):
        ep      = st.session_state.eps[i]
        ep_name = ep.get("name") or f"Episode_{i+1}"
        overall.progress(step_n / n_valid, text=f"{step_n+1}/{n_valid}: {ep_name}…")
        holder  = st.empty()

        def make_bcb(h, nm):
            def cb(pct, msg):
                try:
                    h.progress(min(int(pct), 100) / 100, text=f"{nm}: {msg}")
                except Exception:
                    pass
            return cb

        result = process_episode_from_bytes(
            st.session_state.ep_video_bytes[i],
            st.session_state.ep_video_names.get(i, "video.mp4"),
            st.session_state.ep_srt_bytes[i],
            st.session_state.ep_srt_names.get(i, "subtitle.srt"),
            ep_name, mode, "/tmp/merged_videos",
            progress_cb=make_bcb(holder, ep_name)
        )

        if result["success"]:
            ok_cnt += 1
            holder.success(f"✓ {ep_name} — {result.get('size_str','')}")
            # store output bytes
            st.session_state.ep_output_bytes[i] = result["output_bytes"]
            st.session_state.ep_output_names[i] = result["filename"]
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

    overall.progress(1.0, text=f"Done — {ok_cnt} ok, {fail_cnt} failed")
    st.markdown(
        f'<div class="sum-box">Batch done: <strong>{ok_cnt}</strong> completed, '
        f'<strong>{fail_cnt}</strong> failed</div>',
        unsafe_allow_html=True,
    )
    st.rerun()

st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
#  DOWNLOAD ALL COMPLETED
# ═══════════════════════════════════════════════════════════════════
has_completed = any(
    isinstance(ep, dict)
    and isinstance(ep.get("job"), dict)
    and ep["job"].get("status") == "completed"
    and st.session_state.ep_output_bytes.get(i)
    for i, ep in enumerate(st.session_state.eps)
)

if has_completed:
    st.markdown(
        '<div class="merger-card">'
        '<div class="merger-title">⬇ Download All Completed</div>',
        unsafe_allow_html=True,
    )
    for i, ep in enumerate(st.session_state.eps):
        if not isinstance(ep, dict):
            continue
        job = ep.get("job")
        if not isinstance(job, dict) or job.get("status") != "completed":
            continue
        out_bytes = st.session_state.ep_output_bytes.get(i)
        out_fname = st.session_state.ep_output_names.get(i, f"episode_{i+1}.mp4")
        if not out_bytes:
            continue
        st.download_button(
            label=f"⬇ {ep.get('name', out_fname)} — {format_file_size(len(out_bytes))}",
            data=out_bytes,
            file_name=out_fname,
            mime="video/mp4",
            key=f"dlall_{i}_{uuid.uuid4().hex[:6]}",
            use_container_width=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)


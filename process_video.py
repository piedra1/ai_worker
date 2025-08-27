import cv2
import os
import subprocess

def anonymize_video(input_path, output_path):
    print("Hello anonymize_video")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Load face cascade from OpenCV package
    cascade_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
    print(f"[DEBUG] Using cascade: {cascade_path}")

    # Load cascade
    face_cascade = cv2.CascadeClassifier(cascade_path)

    # Check if loaded correctly
    if face_cascade.empty():
        raise ValueError(f"Failed to load Haar cascade from {cascade_path}")

    cap = cv2.VideoCapture(input_path)
    if not cap.isOpened():
        raise ValueError(f"Could not open input video: {input_path}")

    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 640)
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 480)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0

    # Temporary file for OpenCV output
    temp_output = output_path.replace(".mp4", "_raw.mp4")

    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(temp_output, fourcc, fps, (width, height))

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        # Detect faces
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        faces = face_cascade.detectMultiScale(gray, scaleFactor=1.3, minNeighbors=5)

        # Blur only faces
        for (x, y, w, h) in faces:
            roi = frame[y:y+h, x:x+w]
            roi_blurred = cv2.GaussianBlur(roi, (51, 51), 30)
            frame[y:y+h, x:x+w] = roi_blurred

        out.write(frame)

    cap.release()
    out.release()
    print(f"[✔] Raw anonymized video saved to: {temp_output}")

    # 🔧 Re-encode with ffmpeg to make it streamable
    try:
        cmd = [
            "ffmpeg",
            "-y",  # overwrite if exists
            "-i", temp_output,
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-c:a", "aac",
            "-movflags", "+faststart",
            output_path
        ]
        subprocess.run(cmd, check=True)
        print(f"[✔] Optimized video saved to: {output_path}")
        os.remove(temp_output)  # clean up
    except Exception as e:
        print(f"[✖] ffmpeg re-encoding failed: {e}")
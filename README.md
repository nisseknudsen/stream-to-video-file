# RTSP Stream to Video File

Accepts an RTSP stream and saves it to a video file. The video file is then send to another provider for further
processing, like uploading to storage etc.

Processes at maximum 5 streams in parallel. Timesout after 2 seconds of no new frames. The video is stored in an `mkv`
container.
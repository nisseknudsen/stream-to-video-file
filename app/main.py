import uuid
import os
import logging
import subprocess
from urllib.parse import urlparse, urlunparse, urlencode, parse_qs
from concurrent.futures import ThreadPoolExecutor

from make87_messages.core.header_pb2 import Header
from make87_messages.file.simple_file_pb2 import RelativePathFile
from make87_messages.primitive.bool_pb2 import Bool
from make87_messages.transport.rtsp_pb2 import RTSPRequest

import make87

# Set the maximum number of concurrent ffmpeg processing threads.
MAX_WORKERS = 5


def transform_url(url: str) -> str:
    """
    Transform the given URL into a file path in the format:
      {track}/{starttime}_{endtime}.mkv
    """
    url = url.replace("&amp;", "&")
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split("/")
    if len(path_parts) < 4:
        raise ValueError("Invalid URL structure; cannot find track ID.")

    track = path_parts[3]
    query_params = parse_qs(parsed_url.query)
    starttime = query_params.get("starttime", [None])[0]
    endtime = query_params.get("endtime", [None])[0]

    if not starttime or not endtime:
        raise ValueError("Missing starttime or endtime in URL.")

    return f"{track}/{starttime}_{endtime}.mkv"


def insert_credentials(url: str, username: str, password: str) -> str:
    """
    Insert credentials into the given URL.
    """
    parsed = urlparse(url)
    netloc = f"{username}:{password}@{parsed.hostname}"
    if parsed.port:
        netloc += f":{parsed.port}"
    new_parsed = parsed._replace(netloc=netloc)
    return str(urlunparse(new_parsed))


def build_url(endpoint) -> str:
    """
    Build a URL from an Endpoint message.
    """
    protocol = endpoint.protocol
    host = endpoint.host
    port = endpoint.port
    path = endpoint.path
    if not path.startswith("/"):
        path = "/" + path

    query = urlencode(endpoint.query_params) if endpoint.query_params else ""
    netloc = f"{host}:{port}" if port else host

    return str(urlunparse((protocol, netloc, path, "", query, "")))


def extract_path_and_query(url: str) -> str:
    parsed = urlparse(url)
    resource = parsed.path
    if parsed.query:
        resource += "?" + parsed.query
    return resource.lstrip("/")


def ffmpeg_thread(url: str, output_file: str, message: RTSPRequest, file_release_endpoint):
    """
    Function to run ffmpeg in a thread and handle file upload after processing.
    """
    command = ["ffmpeg", "-rtsp_transport", "tcp", "-timeout", "2000000", "-i", url, "-c", "copy", output_file]
    try:
        subprocess.run(command, check=True)
        with open(output_file, "rb") as f:
            file_bytes = f.read()
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing ffmpeg command: {command}")
        logging.error(f"Error details: {e}")
        file_bytes = None
    else:
        try:
            os.remove(output_file)
        except FileNotFoundError:
            pass

    upload_file = RelativePathFile(
        header=make87.header_from_message(Header, message=message, append_entity_path="upload"),
        data=file_bytes,
        path=transform_url(url),
    )
    file_release_endpoint.request(upload_file)


def main():
    make87.initialize()

    # Create a thread pool executor with a fixed maximum number of worker threads.
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    provider = make87.get_provider(
        name="RTSP_RECORDING_JOB", requester_message_type=RTSPRequest, provider_message_type=Bool
    )

    file_release_endpoint = make87.get_requester(
        name="FILE_UPLOAD", requester_message_type=RelativePathFile, provider_message_type=Bool
    )

    def callback(message: RTSPRequest) -> Bool:
        # Build the URL and inject credentials if available.
        url = build_url(message.endpoint)
        if message.HasField("basic_auth"):
            username = message.basic_auth.username
            password = message.basic_auth.password
            url = insert_credentials(url, username, password)
        elif message.HasField("digest_auth"):
            username = message.digest_auth.username
            password = message.digest_auth.password
            url = insert_credentials(url, username, password)

        # Create a unique file name.
        output_file = f"{uuid.uuid4()}.mkv"

        # Queue the ffmpeg job in the thread pool.
        executor.submit(ffmpeg_thread, url, output_file, message, file_release_endpoint)

        # Return success immediately.
        return Bool(
            header=make87.header_from_message(Header, message=message, append_entity_path="success"),
            value=True,
        )

    provider.provide(callback)
    make87.loop()


if __name__ == "__main__":
    main()

import uuid

from make87_messages.core.header_pb2 import Header
from make87_messages.file.simple_file_pb2 import RelativePathFile
from make87_messages.transport.rtsp_pb2 import RTSPRequest

import make87
import subprocess
from urllib.parse import urlparse, urlunparse, urlencode


def insert_credentials(url: str, username: str, password: str) -> str:
    """
    Insert credentials into the given URL.
    For example, turns:
      rtsp://host/path
    into:
      rtsp://username:password@host/path
    """
    parsed = urlparse(url)
    # Build the new netloc with credentials.
    netloc = f"{username}:{password}@{parsed.hostname}"
    if parsed.port:
        netloc += f":{parsed.port}"
    new_parsed = parsed._replace(netloc=netloc)
    return str(urlunparse(new_parsed))


def build_url(endpoint) -> str:
    """
    Build a URL from an Endpoint message.

    The Endpoint message has:
      - protocol: e.g. "rtsp"
      - host: the server IP or domain name.
      - port: the port number.
      - path: the resource path.
      - query_params: a map of query parameters.
    """
    protocol = endpoint.protocol
    host = endpoint.host
    port = endpoint.port
    path = endpoint.path
    # Ensure the path starts with a slash.
    if not path.startswith("/"):
        path = "/" + path

    # Encode query parameters if they exist.
    query = urlencode(endpoint.query_params) if endpoint.query_params else ""

    # Build the network location. Only include port if provided.
    netloc = f"{host}:{port}" if port else host

    return str(urlunparse((protocol, netloc, path, "", query, "")))


def extract_path_and_query(url: str) -> str:
    parsed = urlparse(url)
    # Combine the path and query string.
    resource = parsed.path
    if parsed.query:
        resource += "?" + parsed.query
    # Optionally, remove the leading slash if you don't want it.
    return resource.lstrip("/")


def main():
    make87.initialize()
    provider = make87.get_provider(
        name="RTSP_RECORDING_JOB", requester_message_type=RTSPRequest, provider_message_type=RelativePathFile
    )

    def callback(message: RTSPRequest) -> RelativePathFile:
        # Build URL from the endpoint message fields.
        url = build_url(message.endpoint)

        # Inject authentication credentials if provided.
        if message.HasField("basic_auth"):
            username = message.basic_auth.username
            password = message.basic_auth.password
            url = insert_credentials(url, username, password)
        elif message.HasField("digest_auth"):
            username = message.digest_auth.username
            password = message.digest_auth.password
            url = insert_credentials(url, username, password)

        # Specify the output file name.
        output_file = f"{uuid.uuid4()}.mkv"

        # Build the ffmpeg command.
        command = ["ffmpeg", "-rtsp_transport", "tcp", "-timeout", "5000000", "-i", url, "-c", "copy", output_file]

        try:
            # Execute the ffmpeg command.
            subprocess.run(command, check=True)
            with open(output_file, "rb") as f:
                file_bytes = f.read()
        except subprocess.CalledProcessError as e:
            print(f"Error executing ffmpeg command: {command}")
            print(f"Error details: {e}")
            file_bytes = None

        return RelativePathFile(
            header=make87.header_from_message(Header, message=message, append_entity_path="response"),
            data=file_bytes,
            path=output_file,
        )

    provider.provide(callback)
    make87.loop()


if __name__ == "__main__":
    main()

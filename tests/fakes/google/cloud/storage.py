"""Fake GCS: each upload becomes a file under $FAKE_GCS_DIR/<bucket>/<path>."""
import os
import pathlib

from google.api_core.exceptions import PreconditionFailed, ServiceUnavailable

ROOT = pathlib.Path(os.environ["FAKE_GCS_DIR"])


def _take(fault: str) -> bool:
    """Use up one injected fault. The count lives in a file so it carries
    across consumer restarts."""
    f = ROOT / f".fault_{fault}"
    n = int(f.read_text()) if f.exists() else 0
    if n > 0:
        f.write_text(str(n - 1))
        return True
    return False


class Blob:
    def __init__(self, bucket: str, name: str):
        self.bucket, self.name = bucket, name

    def upload_from_string(self, data, content_type=None, if_generation_match=None, **_):
        if _take("fail"):
            raise ServiceUnavailable("simulated GCS outage")
        dest = ROOT / self.bucket / self.name
        if if_generation_match == 0 and dest.exists():
            raise PreconditionFailed(f"{self.name} already exists")
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".tmp")
        tmp.write_text(data)
        tmp.rename(dest)
        if _take("lose_response"):
            raise PreconditionFailed(f"{self.name} already exists (retry after lost response)")


class Bucket:
    def __init__(self, name: str):
        self.name = name

    def blob(self, name: str) -> Blob:
        return Blob(self.name, name)


class Client:
    def bucket(self, name: str) -> Bucket:
        return Bucket(name)

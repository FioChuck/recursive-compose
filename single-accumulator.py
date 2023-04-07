import io
from concurrent.futures import Executor, Future
from itertools import count
from logging import getLogger
from os import stat
from sys import stdin
from time import sleep, time
from typing import Iterable, List, Any

from google.cloud import storage

from gcsfast.libraries.gcs import get_gcs_client
from gcsfast.libraries.thread import BoundedThreadPoolExecutor
from gcsfast.libraries.utils import b_to_mb

LOG = getLogger(__name__)


def generate_composition_chunks(slices: List,
                                chunk_size: int = 31) -> Iterable[List]:
    """Given an indefinitely long list of blobs, return the list in 31-item chunks.

    Arguments:
        slices {List} -- A list of blobs, which are slices of a desired final blob.

    Returns:
        Iterable[List] -- An iteration of 31-item chunks of the input list.

    Yields:
        Iterable[List] -- A 31-item chunk of the input list.
    """
    while len(slices):
        chunk = slices[:chunk_size]
        yield chunk
        slices = slices[chunk_size:]


def compose(object_path: str, slices: List[storage.Blob],
            client: storage.Client, executor: Executor) -> storage.Blob:
    """Compose an object from an indefinite number of slices. Composition is
    performed single-threaded with the final object acting as an
    accumulator. Cleanup is performed concurrently using the provided
    executor.

    Arguments:
        object_path {str} -- The path for the final composed blob.
        slices {List[storage.Blob]} -- A list of the slices that should
            compose the blob, in order.
        client {storage.Client} -- A Cloud Storage client to use.
        executor {Executor} -- A concurrent.futures.Executor to use for
            cleanup execution.
    Returns:
        storage.Blob -- The composed blob.
    """
    LOG.info("Composing")
    final_blob = storage.Blob.from_string(object_path)
    final_blob.upload_from_file(io.BytesIO(b''), client=client)

    for chunk in generate_composition_chunks(slices):
        chunk.insert(0, final_blob)
        final_blob.compose(chunk, client=client)
        delete_objects_concurrent(chunk[1:], executor, client)
        sleep(1)  # can only modify object once per second

    return final_blob


def delete_objects_concurrent(blobs, executor, client) -> None:
    """Delete Cloud Storage objects concurrently.

    Args:
        blobs (List[storage.Blob]): The objects to delete.
        executor (Executor): An executor to schedule the deletions in.
        client (storage.Client): Cloud Storage client to use.
    """
    for blob in blobs:
        LOG.debug("Deleting slice {}".format(blob.name))
        executor.submit(blob.delete, client=client)
        sleep(.005)  # quick and dirty ramp-up (Sorry, Dijkstra.)

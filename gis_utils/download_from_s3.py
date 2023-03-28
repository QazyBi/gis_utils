from pathlib import Path

from minio import Minio
from fire import Fire
from pydantic import validate_arguments
from tqdm import tqdm
from functools import partial
import threading

KB = 1024
MB = 1024 * KB


def download_object(
    name,
    size,
    dst_path,
    endpoint,
    access_key,
    secret_key,
    region,
    bucket,
    object_path,
    chunk_size=2 * MB,
):
    inner_client = Minio(
        endpoint=endpoint, access_key=access_key, secret_key=secret_key, region=region
    )
    downloaded = 0 * MB
    _dst_path = dst_path / "_".join(Path(name).stem.split('_')[:-1]) / Path(name).name
    _dst_path.parent.mkdir(exist_ok=True, parents=True)

    with open(_dst_path, "wb") as file_data, tqdm(
        desc=f"{_dst_path.name}",
        total=size,
        dynamic_ncols=True,
        leave=False,
        mininterval=1,
        unit="B",
        unit_scale=True,
        unit_divisor=KB,
    ) as pbar:
        while downloaded < size:
            length = (
                chunk_size if (size - downloaded) >= chunk_size else size - downloaded
            )
            data = inner_client.get_object(
                bucket,
                object_name=name,  # str(Path(object_path, Path(name).name))
                offset=downloaded,
                length=length,
            )
            newly_downloaded = 0
            for d in data:
                newly_downloaded += file_data.write(d)
            downloaded += newly_downloaded
            pbar.update(newly_downloaded)

    if downloaded != size:
        dst_path.unlink(missing_ok=True)
        raise Exception(
            f"File error: size of '{_dst_path}' {downloaded} bytes, expected {size} bytes"
        )


@validate_arguments
def download(
    url: str,
    access_key: str,
    secret_key: str,
    object_path: str,
    dst_path: Path,
    region: str = "ru-1",
    num_workers: int = 4,
):
    """
    url - contains both endpoint and object address
    """
    endpoint = url.split("://")[0]

    client = Minio(
        endpoint=endpoint, access_key=access_key, secret_key=secret_key, region=region
    )
    # print(list(client.list_objects('nspd')))
    # return
    bucket = "nspd"  # "dataset"
    object_path = "/cofp/Республика Татарстан 2000_МСК_UTM_39N/Казань"
    # object_path = "dataset_9/class_multiclass/Raster"
    # dst_path = Path("/home/qazybek/temp")
    dst_path.mkdir(exist_ok=True, parents=True)

    response = client.list_objects(bucket, prefix=object_path, recursive=True)
    arguments = [(i._object_name, i._size) for i in response]
    for i in range(0, len(arguments), num_workers):
        threads = []
        for argument in arguments[i : (i + 1) * num_workers]:
            thread = threading.Thread(
                target=partial(
                    download_object,
                    dst_path=dst_path,
                    endpoint=endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    region=region,
                    bucket=bucket,
                    object_path=object_path,
                ),
                args=(argument[0], argument[1]),
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()


from urlpath import URL


def download_items(endpoint, access_key, secret_key, region, bucket, object_path, arguments, dst_path, num_workers):
    for i in range(0, len(arguments), num_workers):
        threads = []
        for argument in arguments[i : (i + 1) * num_workers]:
            thread = threading.Thread(
                target=partial(
                    download_object,
                    dst_path=dst_path,
                    endpoint=endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    region=region,
                    bucket=bucket,
                    object_path=object_path,
                ),
                args=(argument[0], argument[1]),
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()


def download_from_list(
    url: str,
    access_key: str,
    secret_key: str,
    list_file_path,
    dst_path: Path,
    region: str = "ru-1",
    num_workers: int = 16,  # 4
):
    """
        >>> python download_data.py --url minio-prod.arm.geocode.tech/newest/sentinel/\
                --access_key leshoz --secret_key IlrdVceOYsABLd83\
                --list_file_path some/path\
                --dst_path /some/path\
                --num_workers 32
    """
    url_obj = URL(url)
    target_objects = parse_list_file_s2(list_file_path)
    print(len(target_objects))
    endpoint, bucket, object_path = url_obj.anchor, url_obj.parts[2], "/".join(url_obj.parts[3:])  # skip anchor and bucket
    
    endpoint_str = str(endpoint).removeprefix("https://")
    bucket = str(bucket).removeprefix('/')
    object_path += '/'
 
    client = Minio(
        endpoint=endpoint_str, access_key=access_key, secret_key=secret_key, region=region
    )
    response = client.list_objects(bucket
                                   , prefix=object_path)  # , recursive=True

    # arguments = [Path(i._object_name).stem for i in response ] # if Path(i._object_name).stem in target_objects]  #  if Path(i._object_name).stem.removesuffix('.SAFE') in target_objects
    arguments = [(i._object_name, i._size) for i in response if Path(i._object_name).stem.removesuffix('_48N') in target_objects]

    # download all files into one folder
    download_items(endpoint_str, access_key, secret_key, region, bucket, object_path, arguments, Path(dst_path), num_workers)

    # folders
    return  # following code requires some refactoring
    for arg in arguments:
        response = client.list_objects(bucket, prefix=f"{arg}", recursive=True)
        files = [(i._object_name, i._size) for i in response if "GRANULE" in i._object_name and "IMG_DATA" in i._object_name and ("B08.jp2" in i._object_name or "B02.jp2" in i._object_name or "B03.jp2" in i._object_name or "B04.jp2" in i._object_name)]
        # from pprint import pprint
        download_items(endpoint_str, access_key, secret_key, region, bucket, object_path, files, Path(dst_path, arg.split('/')[-2]), num_workers)
    return arguments


from pydantic import FilePath
@validate_arguments
def parse_list_file_s2(file_path: FilePath) -> list[str]:
    with open(file_path) as f:
        rows = f.read().split('\n')
    rows = [r for r in rows if r != '']  # r.startswith('S2')
    return rows


def find_diff(file, path, diff_type='left'):
    folders = list(Path(path).iterdir())
    folders = [f.name for f in folders]

    rows = parse_list_file_s2(file)
    rows = [r + '.SAFE' for r in rows]
    if diff_type=='left':
        return set(folders) - set(rows)  
    else:
        return set(rows) - set(folders)

if __name__ == "__main__":
    Fire()  # download

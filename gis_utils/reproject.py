# standard library
from functools import partial
from pathlib import Path
from typing import Callable, List, Union
import logging
from multiprocessing import Pool
import shutil

# third party libraries
from pydantic import FilePath, DirectoryPath, validate_arguments
from tqdm import tqdm
import rasterio as rio
from rasterio.warp import Resampling, calculate_default_transform, reproject
# from tqdm.contrib.concurrent import thread_map
# import geopandas as gpd

# local modules
# from .abstract import AbstractOperation
# from .utils import get_new_filepath, validate_file_extensions, new_dst_path

# # todo:
# class Reprojector(AbstractOperation):
#     """Class for doing reprojection of rasters
#     """
#     def __init__(self,
#                  file_type: str,
#                  name_constructor: Callable[[Path], Path],
#                  src_path: Union[str, Path],
#                  dst_path: Union[str, Path],
#                  dst_crs: str = 'EPSG:4326',
#                  leave_pbar: bool = False,
#                  max_workers: int = 8):
#         self.file_type = file_type
#         self.name_constructor = name_constructor
#         self.dst_crs = dst_crs
#         self.src_path = Path(src_path)
#         self.dst_path = Path(dst_path)
#         self.leave_pbar = leave_pbar
#         self.valid_extensions = ['.jp2', '.tif', '.shp']  # TODO: add more
#         self.max_workers = max_workers

## 
# def process(self, files: List[Path]) -> List[Path]:
#     """Function reprojects rasters/geometries and saves result into separate file

#     Args:
#         files: A list of files to be processed.

#     Returns:
#         A list of files that were created after processing.
#     """
#     # check extensions
#     validate_file_extensions(files, self.valid_extensions)
#     # run file processing using multithreading
#     processed_files = thread_map(
#         partial(self.process_file, src_path=self.src_path, dst_path=self.dst_path),
#         files,
#         max_workers=self.max_workers,
#         leave=self.leave_pbar,
#         desc="reprojecting"
#     )

#     return list(processed_files)

# def process_file(self, file: Path, src_path: Path, dst_path: Path):
#     """Function to reproject raster/geometry files

#     Args:
#         file: A file to reproject
#         src_path: A path to the source directory with files
#         dst_path: A path where to store reprojected file

#     Returns:
#         A path to the reprojected file.
#     """
#     # create destination path preserving original directory structure
#     dst_path = new_dst_path(file, src_path, dst_path)
#     new_filename = get_new_filepath(file, dst_path, self.name_constructor)
#     new_filename.parent.mkdir(parents=True, exist_ok=True)

#     if new_filename.exists():
#         return new_filename

#     if self.file_type == "raster":
#         reproject_raster(file, new_filename, self.dst_crs)
#     elif self.file_type == "geometry":
#         reproject_geom_file(file, new_filename, self.dst_crs)
#     else:
#         raise NotImplementedError("This type of operation is not supported")
#     return new_filename
##

# working
# def reproject_geom_file(src_path: Path, dst_path: Path, dst_crs):
#     """Function to reproject geometry file.

#     Args:
#         src_path: A path to the file
#         dst_path: A path where to store reprojected file
#         dst_crs: A crs to which to reproject the source file

#     Returns:
#         None
#     """
#     data = gpd.read_file(src_path)
#     data = data.to_crs(dst_crs)
#     data.to_file(dst_path)


@validate_arguments
def reproject_raster(src_path: FilePath, dst_path: Path, dst_crs: str = 'EPSG:4326'):
    """Function to reproject a raster file.

    Args:
        src_path: A path to the file
        dst_path: A path where to store reprojected file
        dst_crs: A crs to which to reproject the source file

    Returns:
        None
    """
    if dst_path.is_dir():
        dst_path /= src_path.name
    
    # read the raster
    with rio.open(src_path) as src:
        if src.crs == dst_crs:
            shutil.copy(src_path, dst_path)
        else:
            # calculate transformation array and shape of reprojected raster
            transform, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds)
            # prepare metadata for the destination raster
            kwargs = src.meta.copy()
            kwargs.update({
                'driver': 'GTiff',
                'crs': dst_crs,
                'transform': transform,
                'width': width,
                'height': height,
                'count': src.count
            })
            # open destination raster
            with rio.open(dst_path, 'w', **kwargs) as dst:
                # do each band reprojection
                for i in range(1, src.count + 1):
                    reproject(
                        source=rio.band(src, i),
                        destination=rio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=kwargs['transform'],
                        dst_crs=kwargs['crs'],
                        resampling=Resampling.nearest
                    )

def process_folder(folder, dst_path, dst_crs):
    for file in folder.glob("*.tif"):
        file_dst_path = dst_path / folder.name / file.name
        file_dst_path.parent.mkdir(exist_ok=True, parents=True)

        if file_dst_path.exists():
            continue

        try:
            # logging.info(f"reprojecting the file: {file}")
            print(f"reprojecting the file: {file}")
            reproject_raster(file, file_dst_path, dst_crs)
            print(f"finished reprojecting the file: {file}")
        except Exception as e:  # todo: refactor make error handling more sophisticated
            # logging.error(f"Unable to reproject the file: {file}. The error is {e}")
            print(f"Unable to reproject the file: {file}. The error is {e}")

@validate_arguments
def _reproject(src_path: DirectoryPath, dst_path: Path, dst_crs: str = 'EPSG:4326'):
    """
        Usage:

        >>> python reproject.py --src_path /home/qazybek/GIS_qazybek/projects/roads/data/raw-data/Raster\
                                --dst_path /home/qazybek/GIS_qazybek/projects/roads/data/interim-data/240123-rawdata-reprojected\
                                --dst_crs 'EPSG:4326'
    """
    if not dst_path.exists():
        dst_path.mkdir(parents=True)

    folders = src_path.iterdir()
    folders = sorted(folders, key=lambda x: x.name)
    with Pool(32) as p:
        p.map(
            partial(
                process_folder,
                dst_path=dst_path,
                dst_crs=dst_crs,
            ),
            folders
        )


@validate_arguments
def validate_crs(folder: DirectoryPath, expected_crs: str = 'EPSG:4326'):
    for f in folder.iterdir():
        for file in f.glob('*.tif'):
            try:
                crs = rio.open(file).crs
                assert crs == expected_crs, f"{file}: {crs} != {expected_crs}"
            except AssertionError as e:
                print(e)
            except Exception as e:
                print(e)


@validate_arguments
def validate_pix_size(folder: DirectoryPath):
    pix_size = None
    for file in folder.rglob("*.tif"):
        f_pix_size = rio.open(file).transform[0]
        if pix_size is None:
            pix_size = f_pix_size
        else:
            assert f_pix_size == pix_size
            logging.info(f"pix_size is fine: {file}")


if __name__ == "__main__":
    from fire import Fire

    Fire(_reproject)

import asyncio
import logging
import os
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional

import aiofiles
from aiohttp import web
from pydantic import BaseModel

logger = logging.getLogger('download-photos')


class AppParams(BaseModel):
    path: Path
    logging: Optional[bool]
    delay: Optional[int]
    chunk_size: Optional[int]


async def archive(request: web.Request) -> web.StreamResponse:
    app_params = request.app['app_params']
    response = web.StreamResponse()
    archive_hash = request.match_info['archive_hash']
    photo_directory = f"{app_params.path}/{archive_hash}"
    if not os.path.exists(photo_directory):
        raise web.HTTPNotFound(text='Архив не существует или был удален')

    proc = await asyncio.create_subprocess_exec(
        'zip',
        '-r',
        '-',
        '.',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=photo_directory,
    )
    filename = f"{archive_hash}.zip"
    response.headers['Content-Type'] = 'multipart/form-data'
    response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'

    await response.prepare(request)

    chunk_number = 1
    try:
        while not proc.stdout.at_eof():
            data = await proc.stdout.read(n=app_params.chunk_size * 1000)
            logger.debug(f'Sending archive chunk {chunk_number}')
            await response.write(data)
            if app_params.delay:
                logging.debug(f'Waiting for delay {app_params.delay} seconds')
                await asyncio.sleep(app_params.delay)
            chunk_number += 1
    except asyncio.CancelledError as exc:
        logging.warning('Download was interrupted')
        raise exc
    except Exception as exc:
        logging.error(f'Unexpected error: {exc=}')
        raise exc
    finally:
        logging.debug(f'Killing process {proc.pid}')
        try:
            proc.kill()
            await proc.communicate()
        except ProcessLookupError:
            pass
    return response


async def handle_index_page(request: web.Request) -> web.Response:
    async with aiofiles.open('index.html', mode='r', encoding='UTF8') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


if __name__ == '__main__':
    parser = ArgumentParser(
        prog='download-photos',
        description='microservice for downloading photo archives',
    )

    parser.add_argument('-l', '--logging', action='store_true')
    parser.add_argument(
        '-d', '--delay', type=int, default=None, help='delay between chunks in seconds'
    )
    parser.add_argument(
        '-c', '--chunk_size', type=int, default=500, help='chunk_size in kilobytes'
    )
    parser.add_argument(
        '-p', '--path', type=Path, required=True, help='path to catalog with folders'
    )

    app = web.Application()
    app['app_params'] = AppParams.parse_obj(parser.parse_args().__dict__)

    logging.basicConfig(level=logging.DEBUG)
    if not app['app_params'].logging:
        logging.disable()

    app.add_routes(
        [
            web.get('/', handle_index_page),
            web.get('/archive/{archive_hash}/', archive),
        ]
    )
    web.run_app(app)

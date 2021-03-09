import asyncio
import aiodocker
from aiodocker import DockerError


async def main():
    try:
        image = "worker-test"
        docker = aiodocker.Docker()
        print([container for container in await docker.containers.list() if
               container._container.get('Image') == image])
        a = [container for container in await docker.containers.list() if
               container._container.get('Image') == image]


    #     config = {"Image": image, "network": "host", "detach": True, "auto_remove": True,
    #               "Env": ['name_queue=queue3']}
    #     config = {"Image": image, 'HostConfig': {'NetworkMode': 'host',  "AutoRemove": True}, "detach": True,
    #               "Env": ['name_queue=queue3']}
    #     container = await docker.containers.run(config=config)
        await docker.close()
    except DockerError as err:
        print(f'Error starting wgettor:latest, container: {err}')


if __name__ == '__main__':
    asyncio.run(main())

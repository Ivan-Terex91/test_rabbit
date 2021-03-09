import docker

client = docker.from_env()


print(client.__doc__)

# image = "worker-test-aio"
# # command = "-e name_queue=queue3"
# print(client.containers.list(filters={"ancestor": image}))
# #
# a = client.containers.run(image=image, network="host", detach=True, auto_remove=True,
#                           environment={'name_queue': 'queue3'})

# print(a.short_id)
# # container = client.containers.get(a.short_id)
# Container.stop("1b876a9409")
# client.api.stop("3fbedae253")
# client.api.remove_container("3fbedae253")

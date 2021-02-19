import asyncio
import aiohttp
import json
import time
import logging.config
from logging_config import LOGGING_CONFIG
from math import ceil

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger('app_logger')


class CheckAndManagementScaling:
    """Класс вычисления потребности в масштабировании"""

    def __init__(self, queue_settings: dict, rabbit_settings: dict):
        """
        :param queue_settings: параметры очереди
        :param rabbit_settings: настройки rabbitmq
        """
        self.rabbit_name_queue = queue_settings.get('name_queue')
        self.min_consumers = queue_settings.get('min_quantity_consumers')
        self.max_consumers = queue_settings.get('max_quantity_consumers')
        self.default_consumers = queue_settings.get('default_quantity_consumers')
        ############
        self._max_allowed_time = queue_settings.get('max_allowed_time')
        self.max_allowed_time = self._max_allowed_time
        self.messages = 0
        self.consumers = 0
        ############
        self.freq_check = queue_settings.get('freq_check')
        self.user = rabbit_settings.get('user')
        self.password = rabbit_settings.get('password')
        self.host = rabbit_settings.get('host')
        self.port = rabbit_settings.get('port')
        self.vhost = rabbit_settings.get('vhost')
        self.url = f"http://{self.host}:{self.port}/api/queues/{self.vhost}/{self.rabbit_name_queue}"

    async def polling_length_queue_and_count_consumers(self):
        """Метод опроса длины очереди и количества консумеров подписанных на очередь"""
        while True:
            try:
                #  Может стоит перенести в main открывать сессию, а сюда передовать объект сессии
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            auth=aiohttp.BasicAuth(self.user, self.password),
                            url=self.url

                    ) as response:
                        if response.status != 200:
                            raise Exception(
                                f"request to {self.url} returned with error; status code {response.status}, reason {response.reason}")
                        response = await response.json()

                    consumer_count = response.get('consumers')
                    message_count = response.get('messages')
                    ############

                    if message_count > self.messages:
                        self.max_allowed_time = self._max_allowed_time
                        #  Когда в очереди появляются новые сообщения (0 ---> N), то avg_rate_cons не совсем корректно
                        #  использовать сразу
                        if self.messages == 0 and consumer_count > 0:
                            await asyncio.sleep(10)
                            self.messages = message_count
                            continue

                    if message_count == 0:
                        self.messages = message_count
                    ############
                    avg_rate_consumers = response.get('backing_queue_status').get('avg_egress_rate')
                    avg_rate_producers = response.get('backing_queue_status').get('avg_ingress_rate')
                    logger.info(
                        msg=f'Polling {self.rabbit_name_queue}, consumer_count={consumer_count}, message_count={message_count}')
                    #  Получаем рекомендованное количество консумеров
                    recommended_consumer_count = await self.calculation_consumers(consumer_count, message_count,
                                                                                  avg_rate_consumers,
                                                                                  avg_rate_producers)
                    #  Тут отправляем recommended_consumer_count
            except Exception as e:
                # тут логируем ошибки
                logger.error(msg=e)

            await asyncio.sleep(self.freq_check)

    async def calculation_consumers(self, consumer_count: int, message_count: int, avg_rate_consumers: float,
                                    avg_rate_producers: float) -> int:
        """
        Метод рассчёта рекомендуемого количества консумеров
        :param self:
        :param consumer_count: количество консумеров в очереди
        :param message_count: количество сообщений в очереди
        :param avg_rate_consumers: среднее количество обрабатываемых сообщений консумерами в секунду
        :param avg_rate_producers: среднее количество отправляемых сообщений продюссерами в секунду
        :return recommended_consumer_count: рекомендуемое количество консумеров
        """

        if message_count == 0:

            self.max_allowed_time = self._max_allowed_time
            # return 1 or 0 or self.min_consumers
            logger.info(
                f"name_queue={self.rabbit_name_queue}, consumer_count={consumer_count}, recommended_consumer_count=0")
            return 0
        elif consumer_count == 0:
            self.max_allowed_time = self.max_allowed_time - self.freq_check
            recommended_consumer_count = self.min_consumers if self.min_consumers else 1

            logger.info(
                f"name_queue={self.rabbit_name_queue}, consumer_count={consumer_count}, recommended_consumer_count={recommended_consumer_count}"
            )

            if self.max_allowed_time < self._max_allowed_time / 2:
                logger.warning(
                    f'There are no consumers in the {self.rabbit_name_queue} queue, but the messages_count = {message_count}'
                )
                self.max_allowed_time = self._max_allowed_time / 2

            # or self.default_consumers
            return recommended_consumer_count
        else:

            self.max_allowed_time = self.max_allowed_time - self.freq_check
            messages_execution_time = message_count / avg_rate_consumers  # время выполнения задач существующими консумерами
            if self.max_allowed_time < self._max_allowed_time / 2 and self.max_allowed_time < messages_execution_time:
                self.max_allowed_time = self._max_allowed_time / 2

            coefficient_eff_consumers = avg_rate_consumers / avg_rate_producers  # коэффициент эффективности консумеров

            if coefficient_eff_consumers > 1 and messages_execution_time < self.max_allowed_time:
                """Значит очередь расти не будет и существующие консумеры справятся не превышая порогового времени"""
                logger.info(
                    f"name_queue={self.rabbit_name_queue}, existing consumer count enough"
                )
                """Значит, тут можно постепенно уменьшать консумеры"""
                consumer_speed_processing = avg_rate_consumers / consumer_count  # скорость обработки сообщений одним консумером
                avg_rate_consumers_allowed = message_count / self.max_allowed_time  # допустимая скорость обработки консумерами
                recommended_consumer_count = ceil(
                    avg_rate_consumers_allowed / consumer_speed_processing)  # рекомендованное количество консумеров
                logger.info(
                    f"name_queue={self.rabbit_name_queue}, consumer_count={consumer_count}, recommended_consumer_count={recommended_consumer_count}")
                return recommended_consumer_count
            else:
                """Значит очередь растёт и существующие консумеры не справятся в допустимое время"""
                """Тут рассчитываем увеличение консумеров"""
                required_speed_processing = message_count / self.max_allowed_time  # требуемая скорость обработки сообщений
                consumer_speed_processing = avg_rate_consumers / consumer_count  # скорость обработки сообщений одним консумером
                recommended_consumer_count = ceil(
                    required_speed_processing / consumer_speed_processing)  # рекомендованное количество консумеров

                if recommended_consumer_count > self.max_consumers:
                    logger.warning(
                        f"name_queue={self.rabbit_name_queue}, consumer_count={consumer_count}, recommended_consumer_count={recommended_consumer_count}, "
                        f"max_allowed_consumer_count={self.max_consumers}"
                    )
                    return self.max_consumers

                logger.info(
                    f"name_queue={self.rabbit_name_queue}, consumer_count={consumer_count}, recommended_consumer_count={recommended_consumer_count}"
                )
                return recommended_consumer_count

    async def __call__(self, *args, **kwargs):
        await self.polling_length_queue_and_count_consumers()


async def main():
    """Точка входа"""

    with open('test_config.json') as inf:
        settings = json.load(inf)

    # Берём настройки Rabbit
    rabbit_settings = settings.get('Rabbit')
    # Список экземпляров классов check_scaling (очередь-экземпляр)
    list_rabbit_queues = []
    for queue_settings in settings.get('Queues'):
        list_rabbit_queues.append(
            CheckAndManagementScaling(queue_settings, rabbit_settings)
        )

    # Создание списка задач на вычисления потребности в масштабировании
    tasks = [asyncio.create_task(inst_check_scaling()) for inst_check_scaling in list_rabbit_queues]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())

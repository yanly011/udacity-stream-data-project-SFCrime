from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer
import asyncio

async def consume(topic_name):
    c = Consumer({
                  'bootstrap.servers': 'localhost:9092', 
                  'group.id': 0
                 })
    c.subscribe([topic_name])

    while True: 
        messages = c.consume()
        for message in messages:
            if message is None:
                print('Message cannot be found!')
            elif message.error() is not None:
                print(f'Error: {message.error()}')
            else:
                print(f'{message.value()}\n')

    await asyncio.sleep(0.1)

def main():
    client = AdminClient({'bootstrap.servers': 'localhost:9092'})
    try:
        asyncio.run(consume("com.udacity.police-calls"))
    except KeyboardInterrupt as e: 
        print('shutting down')

if __name__ == '__main__':
    main()
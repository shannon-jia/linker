# -*- coding: utf-8 -*-

"""Console script for rps."""

import click
from .log import get_log
from .asyncmongo import AsyncMongo
from .routermq import RouterMQ
from .linker import Linker
from .api import Api
import asyncio


def validate_url(ctx, param, value):
    try:
        return value
    except ValueError:
        raise click.BadParameter('url need to be format: tcp://ipv4:port')


@click.command()
@click.option('--db_uri', default='mongodb://mongo:27017/mean',
              callback=validate_url,
              envvar='DB_URI',
              help='DB URI, ENV: DB_URI, default: mongodb://mongo:27017/mean')
@click.option('--amqp', default='amqp://guest:guest@rabbit:5672//',
              callback=validate_url,
              envvar='SVC_AMQP',
              help='Amqp url, also ENV: SVC_AMQP')
@click.option('--port', default=80,
              envvar='SVC_PORT',
              help='Api port, default=80, ENV: SVC_PORT')
@click.option('--qid', default=1,
              envvar='SVC_QID',
              help='ID for amqp queue name, default=1, ENV: SVC_QID')
@click.option('--debug', is_flag=True)
@click.option('--disable_lamp', is_flag=True)
def main(db_uri, amqp, port, qid, debug, disable_lamp):
    """Publisher for PM-1 with IPP protocol"""

    click.echo("See more documentation at http://www.mingvale.com")

    info = {
        'db_uri': db_uri,
        'api_port': port,
        'amqp': amqp,
    }
    log = get_log(debug)
    log.info('Basic Information: {}'.format(info))

    loop = asyncio.get_event_loop()
    loop.set_debug(0)

    # main process
    try:
        db = AsyncMongo(db_uri)
        db_task = loop.create_task(db.reconnector())
        site = Linker(loop, db)
        router = RouterMQ(outgoing_key='Alarms.newkeeper',
                          routing_keys=['Alarms.keeper'],
                          queue_name='keeper_'+str(qid),
                          url=amqp)
        router.set_callback(site.got_command)
        site.set_publish(router.publish)
        api = Api(loop=loop, port=port, site=site, amqp=router)
        site.start()
        amqp_task = loop.create_task(router.reconnector())
        api.start()
        loop.run_forever()
    except KeyboardInterrupt:
        if amqp_task:
            amqp_task.cancel()
            loop.run_until_complete(amqp_task)
        if db_task:
            db_task.cancel()
            loop.run_until_complete(db_task)
        site.stop()
    finally:
        loop.stop()
        loop.close()

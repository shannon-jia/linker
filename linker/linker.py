# -*- coding: utf-8 -*-

"""Main module."""

import logging
import asyncio
import re
log = logging.getLogger(__name__)

IDENT_PUB = 'Actions.keeper'
IDENT_LINK = 'Actions.linker'
IDENT_MAP = 'Actions.map'
IDENT_SPK = 'Actions.speaker'
IDENT_CCTV = 'Actions.cctv'
IDENT_LAMP = 'Actions.lamp'
TYPE_SPK = 'SPEAKER'
TYPE_CCTV = 'CAMERA'
TYPE_LAMP = 'LAMP'
TYPE_RELAY = 'RELAY'


class Linker():

    def __init__(self, loop, db):
        self.loop = loop or asyncio.get_event_loop()
        self.db = db
        self.links = []
        self.action_list = []

        self.num = 0
        self.num1 = 0

    def set_publish(self, publish):
        if callable(publish):
            self.publish = publish
        else:
            self.publish = None

    def start(self):
        pass

    async def got_command(self, mesg):
        try:
            # self.num1 = self.num1 + 1
            # print('000000000000', self.num1)
            # log.info('Linker received: {}'.format(mesg))
            return await self._do_action(mesg)
        except Exception as e:
            log.error('Linker do_action() exception: {}'.format(e))

    def get_info(self):
        return 'linker'

    async def is_dis(self, mesg):
        if mesg.get('type') != 'Auxiliary Input':
            dis = await self.get_reference(mesg.get('name'),
                                           mesg.get('remark'),
                                           mesg.get('offset'))
            return dis

    async def is_act(self, mesg):
        if mesg.get('type') in ('Auxiliary Input',  'Cable Alarm'):
            act = await self.get_link(mesg.get('name'), mesg.get('offset'))
            return act

    async def _do_action(self, mesg):
        _msg = {}
        _msg['alarmType'] = mesg.get('type').upper()
        _msg['offset'] = mesg.get('offset')
        _msg['time_stamp'] = mesg.get('time_stamp')
        _msg['name'] = mesg.get('name')
        _msg['status'] = 'OCCURRED'
        _msg['selectedUser'] = []
        _msg['level'] = mesg.get('level', "")
        _msg['notes'] = ""
        _msg['system'] = int(mesg.get('name').split('_')[1])
        _msg['counter'] = 1
        _msg['createdTime'] = [mesg.get('time_stamp')]
        _msg['detail'] = mesg.get('detail')
        _msg['actions'] = []
        _msg['description'] = ""
        dis = await self.is_dis(mesg)
        act = await self.is_act(mesg)
        if  mesg.get('type') == 'Comm Fail':
            _msg['latlng'] = await self.get_device(mesg.get('name'))
            if dis:
                _msg['description'] = '围界标定{}米 {} 设备通讯失败'.format(dis, mesg.get('name'))
        elif mesg.get('type') == 'Enclosure Tamper':
            _msg['latlng'] = await self.get_device(mesg.get('name'))
            if dis:
                _msg['description'] = '围界标定{}米 {} 防拆开关打开'.format(dis, mesg.get('name'))
        elif mesg.get('type') == 'Cable Fault':
            _msg['latlng'] = await self.get_device(mesg.get('name'))
            if dis:
                _msg['description'] = '围界标定{}米 PM{} 端电缆故障'.format(dis,
                                                                        mesg.get('name')[3:])
        elif mesg.get('type') == 'Auxiliary Input':
            _msg['latlng'] = await self.get_sensor(mesg.get('name'))[0]
            if act:
                _msg['actions'] = act
            _msg['description'] = await self.get_sensor(mesg.get('name'))[1]
        else:
            _msg['latlng'] = await self.get_segment(mesg.get('name'), mesg.get('detail'))
            if act:
                _msg['actions'] = act
            if dis:
                _msg['description'] ='围界标定{}米报警'.format(dis)
        if _msg['latlng'] is not None:
            # print(_msg)
            # self.send(_msg)
            await self.insert_alarm(_msg)

    async def insert_alarm(self, data):
        createdTime = []
        collection = 'alarms'

        conditions = {"name": data.get('name'),
                      "offset": data.get('offset')}
        alarm_list = await self.find(collection, conditions)
        if alarm_list:
            alarm = alarm_list[0]
            if data.get('alarmType') in ('COMM FAIL', 'ENCLOSURE TAMPER', 'CABLE FAULT'):
                return
            for i in alarm.get('createdTime'):
                createdTime.append(i)
            createdTime.append(data.get('time_stamp'))
            content = {"$set": {"time_stamp": str(data.get('time_stamp')),
                                "counter": alarm.get('counter')+1,
                                "createdTime": createdTime}}
            self.num = self.num + 1
            self.send(data)
            await self.db.do_update(collection, conditions, content)
            # log.info('UPDATE {} SUCCEED!'.format(data))
        else:
            self.num = self.num + 1
            self.send(data)
            await self.db.do_insert(collection, data)
            # log.info('INSERT {} SUCCEED!'.format(data))
        print('*************', self.num)

    async def find(self, collection, conditions):
        try:
            result = await self.db.do_find(collection, conditions)
        except Exception as e:
            result = None
            log.error('Connect to database has Error: {}'.format(e))
        return result

    async def get_reference(self, sys, pm_info, alarm_point):
        collection = 'references'
        alarm_name = sys.split("_")
        sys = alarm_name[1]
        if alarm_name[0] == 'SEG':
            pm_id = pm_info[2]
        else:
            pm_id = alarm_name[2]
        if len(pm_info) < 4:
            pm_cable = 'A'
        else:
            pm_cable = pm_info[3]
        ref_name = 'REF' + '_' + str(sys) + '_' + str(pm_id)
        conditions = {'name': re.compile(ref_name)}
        reference_list = await self.find(collection, conditions)
        if reference_list:
            reference = reference_list[0]
            pm_distance = reference.get('name').split('_')[3]
            if pm_cable is 'A':
                distance = int(pm_distance) - int(alarm_point) * 1.1
            else:
                distance = int(pm_distance) + int(alarm_point) * 1.1
            dis = str(distance)
        else:
            dis = None
        return dis

    async def get_segment(self, seg_name, offset):
        collection = 'segments'
        conditions = {'name': seg_name}
        segment_list = await self.find(collection, conditions)
        if segment_list:
            segment = segment_list[0]
            if segment.get('status') != 'SECURE':
                return
            latlng1 = segment.get('latlng')[0]
            latlng2 = segment.get('latlng')[1]
            lat = latlng1[0] + (latlng2[0] - latlng1[0]) * offset
            lng = latlng1[1] + (latlng2[1] - latlng1[1]) * offset
            latlng = [lat, lng]
        else:
            latlng = None
        return latlng

    async def get_link(self, link_name, alarm_point):
        actions = []
        collection = 'links'
        conditions = {"name": link_name,
                      "min": {"$lte": int(alarm_point)},
                      "max": {"$gte": int(alarm_point)}}
        link_mult = await self.find(collection, conditions)
        if link_mult:
            for link in link_mult:
                if 'CAM' in link.get('action'):
                    data = link.get('action') + '/' + str(link.get('args'))
                else:
                    data = link.get('action')
                actions.append(data)
        else:
            actions = None
        return actions

    async def get_device(self, address):
        collection = 'devices'
        address = address.split('_')
        dev_type = address[0][:2]
        if address[0] == 'CAB':
            dev_type = 'PM'
        sys = address[1]
        pm_id = address[2]
        conditions = {"system": int(sys),
                      "address": int(pm_id),
                      "dev_type": dev_type}
        device_list = await self.find(collection, conditions)
        if device_list:
            device = device_list[0]
            pm_latlng = device.get('latlng')
        else:
            pm_latlng = None
        return pm_latlng

    async def get_sensor(self, sen_name):
        collection = 'sensors'
        conditions = {"name": sen_name}
        sensor_list = await self.find(collection, conditions)
        if sensor_list:
            sensor = sensor_list[0]
            if sensor.get('status') != 'SECURE':
                return
            sen_latlng = sensor.get('latlng')
            sen_description = sensor.get('title')
        else:
            sen_latlng = None
            sen_description = ""
        return sen_latlng, sen_description



    # def _is_match_rt(self, name, offset, link):
    #     lk_name = link.get('name')
    #     if lk_name is None or lk_name is '':
    #         return False
    #     if lk_name != name:
    #         return False
    #     _lx = name.split('_')
    #     idx = _lx[0].upper()
    #     if idx != 'SEG':
    #         return True
    #     # segment
    #     lk_min = int(link.get('min', 0))
    #     lk_max = int(link.get('max', 199))
    #     if lk_min <= offset and lk_max >= offset:
    #         return True
    #     if lk_min >= offset and lk_max <= offset:
    #         return True
    #     return False

    # async def _do_action_rt(self, mesg):
    #     name = mesg.get('name', None)
    #     event_type = mesg.get('type', None)
    #     if ((name is None) or (event_type is None)):
    #         return None
    #     # detail = mesg.get('detail', None)
    #     offset = int(mesg.get('offset', 0))
    #     try:
    #         links = await self.db.do_find(collection='links',
    #                                       filter={'name': name})
    #     except Exception as e:
    #         log.error('Connect to database has Error: {}'.format(
    #             e))
    #         return None

    #     if not links:
    #         log.info('No link table for {}'.format(name))
    #         return None

    #     self.action_list = []
    #     for link in links:
    #         if self._is_match_rt(name, offset, link):
    #             log.info('------> Match!!!: {}'.format(link))
    #             action = link.get('action')
    #             (ident, act_type) = self._get_ident(action)
    #             log.info('Ident: {} Type: {}'.format(ident, act_type))
    #             if ident is None:
    #                 continue
    #             lnk_msg = {'type': act_type,
    #                        'name': action,
    #                        'status': 'AUTO',
    #                        'args': link.get('args')}
    #             self.send(ident, lnk_msg)
    #         else:
    #             log.info('No action for {}'.format(name))

    #     if len(self.action_list) > 0:
    #         return self.action_list
    #     else:
    #         return None

    # def _get_ident(self, name):
    #     if name is None:
    #         return None
    #     _lx = name.split('_')
    #     if len(_lx) < 2:
    #         return None
    #     idx = _lx[0].upper()
    #     ident = None
    #     act_type = None
    #     if idx == 'SPK':
    #         ident = IDENT_SPK
    #         act_type = TYPE_SPK
    #     elif idx == 'CAM':
    #         ident = IDENT_CCTV
    #         act_type = TYPE_CCTV
    #     elif idx == 'CCTV':
    #         ident = IDENT_CCTV
    #         act_type = TYPE_CCTV
    #     elif idx == 'LAMP':
    #         ident = IDENT_LAMP
    #         act_type = TYPE_LAMP
    #     elif idx == 'PM' or idx == 'PMO' or idx == 'RM' or idx == 'RMO':
    #         ident = IDENT_PUB
    #         act_type = TYPE_RELAY
    #     return (ident, act_type)

    def send(self, rep_msg):
        if callable(self.publish):
            self.publish(rep_msg)

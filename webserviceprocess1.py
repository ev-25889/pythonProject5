""" Это процесс веб-сервиса
модуль обеспечивает прием данных он НСИ

Реализована операция
"""

import pip
import logging
import traceback
import os
import sys
import argparse
from datetime import datetime
import json
from wsgiref.simple_server import WSGIServer
import xml.etree.ElementTree as ET

try:
    import xmltodict
except ModuleNotFoundError as e:
    pip.main(['install', 'xmltodict'])
    import xmltodict

try:
    from lxml import etree, objectify
except ModuleNotFoundError as e:
    pip.main(['install', 'lxml'])
    from lxml import etree

try:
    from spyne import Application, rpc, ServiceBase, \
        Integer, Unicode
except ModuleNotFoundError as e:
    pip.main(['install', 'spyne'])
    from spyne import Application, rpc, ServiceBase, \
        Integer, Unicode

from spyne.error import ResourceNotFoundError
from spyne.model.primitive import Boolean
from spyne.model.primitive import AnyXml
from spyne.model.complex import ComplexModel
from spyne.protocol.soap import Soap12
from spyne.server.wsgi import WsgiApplication
from spyne.error import InternalError
from spyne.model.fault import Fault
from spyne.model.complex import TTableModel

try:
    from sqlalchemy import create_engine
except ModuleNotFoundError as e:
    pip.main(['install', 'psycopg2'])
    pip.main(['install', 'sqlalchemy'])

    from sqlalchemy import create_engine

from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from generallib import projectConfig

# config = projectConfig(os.path.dirname(__file__) + '/')
db = create_engine("postgresql://postgres:1122@localhost/cars_api")
Session = sessionmaker(bind=db)
metadata = MetaData(bind=db)

TableModel = TTableModel(metadata)


def todict(data, nsi_table):
    s = etree.tostring(data)
    doc = xmltodict.parse(s)
    doc1 = doc['x-datagram:x-datagram'][nsi_table]
    return dict(doc1[0])


class UserDefinedContext(object):
    def __init__(self):
        self.session = Session()
        self.nsi_table = None
        self.last = False
        # print('UserDefinedContext called')


def _on_method_call(ctx):
    # print("_on_method_call вызван")
    ctx.udc = UserDefinedContext()


def _on_method_context_closed(ctx):
    # print('_on_method_context_closed вызван')
    if ctx.udc is not None:
        try:
            ctx.udc.session.commit()
            ctx.udc.session.close()
        except Exception as e:
            trace = traceback.format_exc()
            s = 'commit' + str(e) + trace
            logging.error(s)
            sys.exit()


class RoutingHeaderType(ComplexModel):
    _type_info = [
        ('operationType', Unicode),
        ('messageId', Unicode),
        ('correlationId', Unicode),
        ('parentId', Unicode),
        ('sourceId', Unicode),
        ('destinationId', Unicode),
        ('replyDestinationId', Unicode),
        ('ticketDestinationId', Unicode),
        ('async', Boolean),
        ('elementsCount', Integer)
    ]


class ServiceResponseType(ComplexModel):
    callCC = Integer
    callRC = Unicode
    routingHeader = RoutingHeaderType
    datagram = AnyXml


class asyncServiceResponseType(ComplexModel):
    __name__ = 'asyncResponse'
    callCC = Integer
    callRC = Unicode
    routingHeader = RoutingHeaderType
    datagram = AnyXml


class asyncServiceRequestType(TableModel):
    __tablename__ = 'packages'

    operationType = Unicode
    messageId = Unicode(pk=True)
    sourceId = Unicode
    destinationId = Unicode
    last = Boolean
    datagram = AnyXml
    callCC = Integer
    callRC = Unicode

class fillRootRegElType(TableModel):
    __tablename__ = 'discipline'

    # id = Unicode(pk=True)
    external_id = Unicode(pk=True)
    title = Unicode

class fillEduProgramType(TableModel):
    __tablename__ = 'educational_programs'

    external_id = Unicode(pk=True)
    title = Unicode
    direction = Unicode
    code_direction = Unicode
    start_year = Unicode
    end_year = Unicode

class ServiceRequestType(ComplexModel):
    callCC = Integer
    callRC = Unicode
    routingHeader = RoutingHeaderType
    datagram = AnyXml

def choise_table(datagram):
    # получаю список тегов из всей датаграммы
    list_of_tag = list()
    for child in datagram:
        if child.tag not in list_of_tag:
            list_of_tag.append(child.tag)
    # print(list_of_tag)
    if "{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}EducationLevelsHighSchool" in list_of_tag:
        print('EducationLevelsHighSchool')
        return save_ELHS_to_db(datagram=datagram)
   #  if "{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}RootRegistryElement" in list_of_tag:
       # print('RootRegistryElement')
       # save_RRE_to_db(datagram=datagram)
def save_RRE_to_db(datagram):
    # метод возвращает список объектов-дисциплин для сохранения в бд

    list_of_RRE = datagram.findall \
        ("{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}RootRegistryElement")
    if len(list_of_RRE) > 0:
        print('found discipline')
        list_of_RREI = list()
        list_of_RREN = list()
        try:
            for REE in list_of_RRE:
                list_of_RREN.append(REE.find(
                    "{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}RootRegistryElementName").text)
                list_of_RREI.append(REE.find(
                    "{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}ID").text)
                # for sechild in child:
            print(list_of_RREI)
            print(list_of_RREN)
        except Exception:
            print("Ошибка получения данных RRE из запроса")
    else:
        print("didn't find discipline")
    try:
        li = list()
        for i in range(len(list_of_RREI)):
            RRE = fillRootRegElType(
                # id = 1,
                external_id=list_of_RREI[i],
                title=list_of_RREN[i]
            )
            li.append(RRE)
            print(type(RRE))
        return li
    except Exception:
        print("Ошибка сохранения данных в базу")

def save_ELHS_to_db(datagram):
    list_of_ELHS = datagram.findall \
        ("{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}EducationLevelsHighSchool")
    if len(list_of_ELHS) > 0:
        print('found edu Program')
        IDs, titles, directions = list(), list(), list()
        codes, starts, ends = list(), list(), list()
        try:
            for child in list_of_ELHS:
                IDs.append(child.find(
                    "{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}ID").text)
                titles.append(child.find(
                    "{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}EducationLevelsHighSchoolName").text)
                for EduProgramSubject in child.find("{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}SubjectID"):
                    for ID in EduProgramSubject:
                        directions.append(ID.text)
                starts.append(child.find(
                    "{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}EducationLevelsHighSchoolOpenDate").text)
                ends.append(child.find(
                    "{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}EducationLevelsHighSchoolCloseDate").text)
            print(IDs, titles, directions, starts, ends)
        except Exception:
            print(f'Ошибка получения данных ELHS из запроса')
        try:
            li = list()
            for i in range(len(list_of_ELHS)):
                ELHS = fillEduProgramType(
                    external_id=IDs[i],
                    title=titles[i],
                    direction=directions[i],
                    start_year=starts[i][:4],
                    end_year=ends[i][:4]
                )
                li.append(ELHS)
            return li
        except Exception:
            print("Ошибка сохранения данных в базу")
    else:
        print("Edu Program didn't find")

def save_SOE_to_db(datagram):
    list_of_SOE = datagram.findall \
        ("{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}StudentOrderExtract")
    if len(list_of_SOE) > 0:
        print('SOE was found!')
        IDs, students, conflows, flowtypes = list(), list(), list(), list()
        dates, efs, ffs, ds = list(), list(), list(), list()
        try:
            for c in list_of_SOE:
                IDs.append(c.find("{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}ID").text)
                for studentID in c:
                    for student in studentID:
                       students.append(c.find("{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}ID").text)
                conflows.append(c.find("{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}StudentOrderExtractReason").text)
                dates.append(c.find("{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}StudentOrderExtractBeginDate").text)
        except Exception:
            print(Exception)



class ServiceSoapImplService(ServiceBase):
    # @rpc(Unicode, _returns=Unicode)
    @rpc(ServiceRequestType, _returns=ServiceResponseType, _body_style='bare'  # Декоратор "@rpc(...)" определяет
        , _in_variable_names={'insertRequest': 'insertRequest'},  # тип входящих аргументов и исходящих
         _out_message_name='insertResponse')  # ответов
    def insertRequest(ctx, insertRequest):  # аргумент ctx содержит инфу у вх. запросах
        # print(etree.tostring(ctx.in_document))
        __name__ = 'insertRequest'
        # print('Запрос получен')
        try:
            logging.debug(insertRequest.routingHeader.messageId + 'insertRequest')
            # tree = etree.ElementTree(insertRequest)
            print(insertRequest)
            a = asyncServiceRequestType(
                operationType=insertRequest.routingHeader.operationType,
                messageId=insertRequest.routingHeader.messageId,
                sourceId=insertRequest.routingHeader.sourceId,
                destinationId=insertRequest.routingHeader.destinationId,
                datagram=insertRequest.datagram,
                callCC=insertRequest.callCC,
                callRC=insertRequest.callRC
            )
            list_of_MC = insertRequest.datagram.findall \
                ("{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0}StudentOrderExtract")
            for child in save_SOE_to_db(datagram=insertRequest.datagram):
                ctx.udc.sessionadd(child)
            for child in save_ELHS_to_db(datagram=insertRequest.datagram):
                ctx.udc.session.add(child)
            for child in save_RRE_to_db(datagram=insertRequest.datagram):
                ctx.udc.session.add(child)
            # ctx.udc.session.add(a)
            ctx.udc.session.flush()

        except BaseException as er:
            trace = traceback.format_exc()
            logging.error('insertRequest' + str(er) + ' ' + trace)

        insertResponse = ServiceResponseType(
            callCC=0,
            callRC='OK'
        )
        return insertResponse

    @rpc(ServiceRequestType, _returns=ServiceResponseType, _body_style='bare'  # Декоратор "@rpc(...)" определяет
        , _in_variable_names={'updateRequest': 'updateRequest'},  # тип входящих аргументов и исходящих
         _out_message_name='updateResponse')  # ответов
    def updateRequest(ctx, updateRequest):
        __name__ = 'updateRequest'

        try:
            logging.debug(updateRequest.routingHeader.messageId + ' updateRequest')
            a = asyncServiceRequestType(
                operationType=updateRequest.routingHeader.operationType,
                messageId=updateRequest.routingHeader.messageId,
                sourceId=updateRequest.routingHeader.sourceId,
                destinationId=updateRequest.routingHeader.destinationId,
                datagram=updateRequest.datagram,
                callCC=updateRequest.callCC,
                callRC=updateRequest.callRC
            )
            ctx.udc.session.add(a)
            ctx.udc.session.flush()
        except BaseException as er:
            trace = traceback.format_exc()
            logging.error('updateRequest' + str(er) + ' ' + trace)

        updateResponse = ServiceResponseType(
            callCC=0,
            callRC='OK'
        )
        return updateResponse

    @rpc(ServiceRequestType, _returns=ServiceResponseType, _body_style='bare'
        , _in_variable_names={'deleteRequest': 'deleteRequest'},
         _out_message_name='deleteResponse')
    def deleteRequest(ctx, deleteRequest):
        __name__ = 'deleteRequest'
        try:
            logging.debug(deleteRequest.routingHeader.messageId + ' deleteRequest')
            a = asyncServiceRequestType(
                operationType=deleteRequest.routingHeader.operationType,
                messageId=deleteRequest.routingHeader.messageId,
                sourceId=deleteRequest.routingHeader.sourceId,
                destinationId=deleteRequest.routingHeader.destinationId,
                datagram=deleteRequest.datagram,
                callCC=deleteRequest.callCC,
                callRC=deleteRequest.callRC
            )
            ctx.udc.session.add(a)
            ctx.udc.session.flush()

        except BaseException as er:
            trace = traceback.format_exc()
            logging.error('deleteRequest' + str(er) + ' ' + trace)

        deleteResponse = ServiceResponseType(
            callCC=0,
            callRC='OK'
        )
        return deleteResponse

    @rpc(asyncServiceRequestType, _returns=asyncServiceResponseType,
         _body_style='bare', _in_variable_names={'asyncRequest': 'asyncRequest'},
         _out_message_name='asyncResponse')
    def asyncRequest(ctx, asyncRequest):
        __name__ = 'asyncRequest'
        # Записываем присланый пакет в базу данных
        ctx.udc.session.add(asyncRequest)
        ctx.udc.session.flush()

        asyncResponse = asyncServiceResponseType()
        return asyncResponse


# TableModel.Attributes.sqla_metadata.drop_all()
# TableModel.Attributes.sqla_metadata.create_all(
# checkfirst=True)

class WebServiceNSIApp(Application):
    def __init__(self, services, tns, name=None,
                 in_protocol=None, out_protocol=None):
        Application.__init__(self, services, tns, name, in_protocol,
                             out_protocol)

        self.event_manager.add_listener('method_call', _on_method_call)
        self.event_manager.add_listener("method_context_closed",
                                        _on_method_context_closed)

    def call_wrapper(self, ctx):
        # print('call_wrapper called')
        try:
            # print('call_wrapper: ', ctx.service_class.call_wrapper(ctx))
            return ctx.service_class.call_wrapper(ctx)


        except NoResultFound:
            print('NoResultFound')
            raise ResourceNotFoundError(ctx.in_object)

        except Fault as e:
            print('Fault')
            logging.error(e)
            raise

        except Exception as e:
            print('Exeption')
            logging.exception(e)
            raise InternalError(e)


if __name__ == '__main__':
    # You can use any Wsgi server. Here, we chose
    # Python's built-in wsgi server but you're not
    # supposed to use it in production.
    # НАстройка логов.
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true",
                        help="turn on debug mode")
    args = parser.parse_args()
    if args.debug:
        dmm = logging.DEBUG
    else:
        dmm = logging.INFO
    format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    script_path = os.path.dirname(__file__) + '/' + 'logs/webservice.log'
    # logging.basicConfig(filename=script_path, filemode='a', format=format_str,
                        # level=dmm)


    application = WebServiceNSIApp([ServiceSoapImplService],
                                   name='ServiceSoapPort',
                                   tns='http://www.tandemservice.ru/Schemas/Tandem/Nsi/Service',
                                   in_protocol=Soap12(validator='lxml'),
                                   out_protocol=Soap12()
                                   )
    wsgi_app = WsgiApplication(application)
    from wsgiref.simple_server import make_server

    wsgi_application = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_application)
    server.serve_forever()
    """
    if sys.platform == "linux" or sys.platform == "linux2":
        # linux
        import bjoern

        bjoern.run(wsgi_app, '0.0.0.0', config.wsgi_port())
    elif sys.platform == "win32":
        # Windows...
        from wsgiref.simple_server import make_server

        server = make_server('0.0.0.0', config.wsgi_port(), wsgi_app)
        server.serve_forever()
"""

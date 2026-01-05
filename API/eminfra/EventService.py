from typing import Generator
from datetime import datetime, timedelta

from API.eminfra.EMInfraDomain import EventType, IdentiteitKenmerk, EventContext, Event, QueryDTO, \
    SelectionDTO, PagingModeEnum, ExpressionDTO, TermDTO, OperatorEnum, LogicalOpEnum, AssetDTO
from utils.date_helpers import format_datetime


class EventService:
    def __init__(self, requester):
        self.requester = requester

    def get_all_eventtypes(self) -> Generator[EventType]:
        url = "core/api/events/eventtypes"
        json_dict = self.requester.get(url).json()
        yield from [EventType.from_dict(item) for item in json_dict['data']]

    def search_eventcontexts(self, omschrijving: str) -> Generator[EventContext]:
        """
        Search all events linked to a specific context. For example aanlevering DA-2025-00001
        """
        query_dto = QueryDTO(size=100,
                             from_=0,
                             orderByProperty='omschrijving',
                             pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(
                                 expressions=[
                                     ExpressionDTO(
                                         terms=[TermDTO(property='omschrijving', operator=OperatorEnum.CONTAINS,
                                                        value=f'{omschrijving}',
                                                        logicalOp=None)]
                                         , logicalOp=None)]))
        url = "core/api/eventcontexts/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [EventContext.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break

    def search_events(self, asset: AssetDTO = None, created_after: datetime = None, created_before: datetime = None,
                      created_by: IdentiteitKenmerk = None, event_type: EventType = None,
                      event_context: EventContext = None) -> Generator[Event]:
        """
        Search the history of em-infra, called events
        Parameters created_before and created_after have type datetime, but the API only takes into account the datum,
         and not the hours.
        Additional postprocessing filtering outside the function is required to narrow down the events to
         a more restricted time range.

        :param asset: Asset
        :type asset: AssetDTO
        :param created_after: date after which the asset was edited
        :param created_before: date before the asset was edited
        :param created_by: person who created the asset
        :param event_type: type of event
        :param event_context: context of the event
        :return: A generator yielding Event objects.
        """
        if all(p is None for p in (asset.uuid, created_after, created_before, created_by, event_type, event_context)):
            raise ValueError("At least one parameter must be provided.")

        query_dto = QueryDTO(size=100, from_=0, pagingMode=PagingModeEnum.OFFSET,
                             selection=SelectionDTO(expressions=[]))

        if asset.uuid:
            expression = ExpressionDTO(
                terms=[TermDTO(property='objectId', operator=OperatorEnum.EQ, value=f'{asset.uuid}')],
                logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        if created_after:
            expression = ExpressionDTO(
                terms=[TermDTO(property='createdOn', operator=OperatorEnum.GTE, value=format_datetime(created_after))],
                logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        if created_before:
            # workaround: add 1 day and set the operator to strictly lower than.
            created_before += timedelta(days=1)
            expression = ExpressionDTO(
                terms=[TermDTO(property='createdOn', operator=OperatorEnum.LT, value=format_datetime(created_before))],
                logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        if created_by:
            expression = ExpressionDTO(
                terms=[TermDTO(property='createdBy', operator=OperatorEnum.EQ, value=created_by.uuid)],
                logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        if event_type:
            expression = ExpressionDTO(
                terms=[TermDTO(property='type', operator=OperatorEnum.EQ, value=event_type.name)],
                logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        if event_context:
            expression = ExpressionDTO(
                terms=[TermDTO(property='contextId', operator=OperatorEnum.EQ, value=event_context.uuid)],
                logicalOp=LogicalOpEnum.AND)
            query_dto.selection.expressions.append(expression)

        # Set logical operator to None for the first term of the expression
        query_dto.selection.expressions[0].logicalOp = None

        url = "core/api/events/search"
        while True:
            json_dict = self.requester.post(url, data=query_dto.json()).json()
            yield from [Event.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            query_dto.from_ = json_dict['from'] + query_dto.size
            if query_dto.from_ >= dto_list_total:
                break
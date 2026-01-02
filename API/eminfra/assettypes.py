from API.EMInfraDomain import AssettypeDTO, Generator, QueryDTO, PagingModeEnum, SelectionDTO, TermDTO, ExpressionDTO, \
    OperatorEnum


class AssettypesService:
    def __init__(self, requester):
        self.requester = requester

    def get_assettype(self, assettype_uuid: str) -> AssettypeDTO:
        """
        Get assettype by uuid

        :param assettype_uuid:
        :type assettype_uuid: str
        :return:
        :rtype:
        """
        url = f"core/api/assettypes/{assettype_uuid}"
        json_dict = self.requester.get(url).json()
        return AssettypeDTO.from_dict(json_dict)

    def search_assettype(self, uri: str) -> AssettypeDTO:
        """
        Search assettype by URI.
        One single Assettype is returned, based on an exact search of the URI.
        :param uri: assettype URI
        :type uri: str
        :return:
        """
        query_dto = QueryDTO(
            size=1,
            from_=0,
            pagingMode=PagingModeEnum.OFFSET,
            selection=SelectionDTO(
                expressions=[ExpressionDTO(
                    terms=[
                        TermDTO(property='uri', operator=OperatorEnum.EQ, value=uri)
                    ])
                ])
        )
        url = "core/api/assettypes/search"
        json_dict = self.requester.post(url, data=query_dto.json()).json()
        assettypes = [AssettypeDTO.from_dict(item) for item in json_dict['data']]
        if len(assettypes) != 1:
            raise ValueError(f'Exactly one Assettype should be returned when searching for uri: "{uri}" Check URI.')
        return assettypes[0]

    def get_all_assettypes(self, size: int = 100) -> Generator[AssettypeDTO]:
        from_ = 0
        while True:
            url = f"core/api/assettypes?from={from_}&size={size}"
            json_dict = self.requester.get(url).json()
            yield from [AssettypeDTO.from_dict(item) for item in json_dict['data']]
            dto_list_total = json_dict['totalCount']
            from_ = json_dict['from'] + size
            if from_ >= dto_list_total:
                break

    def get_all_legacy_assettypes(self, size: int = 100) -> Generator[AssettypeDTO]:
        yield from [assettype_dto for assettype_dto in self.get_all_assettypes(size)
                    if assettype_dto.korteUri.startswith('lgc:')]

    def get_all_otl_assettypes(self, size: int = 100) -> Generator[AssettypeDTO]:
        yield from [assettype_dto for assettype_dto in self.get_all_assettypes(size)
                    if ':' not in assettype_dto.korteUri]
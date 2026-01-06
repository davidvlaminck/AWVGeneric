from API.eminfra.EMInfraDomain import FeedPage


class FeedService:
    def __init__(self, requester):
        self.requester = requester

    def get_feedproxy_page(self, feed_name: str, page_num: int, page_size: int = 1) -> FeedPage:
        """
        Get a feedproxy page

        :param feed_name:
        :type feed_name: str
        :param page_num:
        :type page_num: int
        :param page_size:
        :type page_size: int
        :return FeedPage
        """
        url = f"feedproxy/feed/{feed_name}/{page_num}/{page_size}"
        json_dict = self.requester.get(url).json()
        return FeedPage.from_dict(json_dict)
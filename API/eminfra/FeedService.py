from API.eminfra.EMInfraDomain import FeedPage


class FeedService:
    def __init__(self, requester):
        self.requester = requester

    def get_feedproxy_page(self, feed_name: str, page_num: int, page_size: int = 1):
        url = f"feedproxy/feed/{feed_name}/{page_num}/{page_size}"
        json_dict = self.requester.get(url).json()
        return FeedPage.from_dict(json_dict)
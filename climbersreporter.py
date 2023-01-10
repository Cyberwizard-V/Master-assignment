from mountain import Mountain
from expedition import Expedition
from climber import Climber
from datetime import datetime


class Reporter:
    # How many climbers are there? -> int
    def total_amount_of_climbers(self) -> int:
        pass

    # What is the highest mountain? -> Mountain
    def highest_mountain(self) -> Mountain:
        pass

    # What is the longest and shortest expedition? -> tuple[Expedition, Expedition]
    def longest_and_shortest_expedition(self) -> tuple[Expedition, Expedition]:
        pass

    # Which expeditons have the most climbers -> tuple[Expedition, ...]
    def expedition_with_most_climbers(self) -> tuple[Expedition, ...]:
        pass

    # Which climbers have made the most expeditions -> tuple[Climber, ...]
    # Which climbers have made the most succesful expeditions -> tuple[Climber, ...]
    def climbers_with_most_expeditions(self, only_succesful: bool = False) -> tuple[Climber, ...]:
        pass

    # Which mountain has the most expeditions -> Mountain
    def mountains_with_most_expeditions(self) -> tuple[Mountain, ...]:
        pass

    # Which expedition was the first expedition? -> Expedition
    # Which expedition was the first successful expedition? -> Expedition
    def get_first_expedition(self, only_succesful: bool = False) -> Expedition:
        pass

    # Which expedition is the latest? -> Expedition
    # Which succesful expedition is the latetst? -> Expedition
    def get_latest_expedition(self, only_succesful: bool = False) -> Expedition:
        pass

    # Which climbers have climbed mountain Z between period X and Y? -> tuple[Climber, ...]
    # Based on given parameter `to_csv = True` should generate CSV file as  `Climbers Mountain Z between X and Y.csv`
    # otherwise it should just return the value as tuple(Climber, ...)
    # CSV example:
    #   Id, first_name, last_name, nationality, date_of_birth
    def get_climbers_that_climbed_mountain_between(self, mountain: Mountain, start: datetime, end: datetime, to_csv: bool = False) -> tuple[Climber, ...]:
        pass

    # Which mountains are located in country X? ->tuple[Mountain, ...]
    # Based on given parameter `to_csv = True` should generate CSV file as  `Mountains in country X.csv`
    # otherwise it should just return the value as tuple(Mountain, ...)
    # CSV example:
    #   Id, name, country, rank, height, prominence, range
    def get_mountains_in_country(self, country: str, to_csv: bool = False) -> tuple[Mountain, ...]:
        pass

    # Which climbers are from country X? -> tuple[Climber, ...]
    # Based on given parameter `to_csv = True` should generate CSV file as  `Climbers in country X.csv`
    # otherwise it should just return the value as tuple(Climber, ...)
    # CSV example:
    #   Id, first_name, last_name, nationality, date_of_birth
    def get_climbers_from_country(self, country: str, to_csv: bool = False) -> tuple[Climber, ...]:
        pass

import json
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from string import ascii_lowercase
from sys import exit
from typing import Optional
from typing import cast

from tqdm import tqdm

from exit_code import ExitCode

COMMON_FIELDS = ["nickname", "height", "weight", "stance", "wins", "losses", "draws"]
STATS_FIELDS = ["slpm", "strAcc", "sapm", "strDef", "tdAvg", "tdAcc", "tdDef", "subAvg"]

FINAL_FIELDS = [
    "name",
    "dateOfBirth",
    "physicalFeatures",
    "stance",
    "record",
    "currentChampion",
    "careerStats",
]

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR_1 = DATA_DIR / "fighters_list"
DATA_DIR_2 = DATA_DIR / "fighter_details"


def is_valid_height(height: Optional[str]) -> bool:
    if height is None:
        return True
    pattern = r"\d{1}' \d{1,2}\""
    match = re.match(pattern, height)
    return isinstance(match, re.Match)


def is_valid_weight(weight: Optional[str]) -> bool:
    if weight is None:
        return True
    pattern = r"\d+ lbs"
    match = re.match(pattern, weight)
    return isinstance(match, re.Match)


def is_valid_reach(reach: Optional[str]) -> bool:
    if reach is None:
        return True
    pattern = r"\d+([.]\d+)?\""
    match = re.match(pattern, reach)
    return isinstance(match, re.Match)


def is_valid_stance(stance: Optional[str]) -> bool:
    return stance is None or stance.lower() in ["orthodox", "southpaw", "switch"]


@dataclass(frozen=True, kw_only=True)
class FighterData1:
    link: str
    lastName: str
    wins: int
    losses: int
    draws: int
    currentChampion: bool
    firstName: Optional[str] = None
    nickname: Optional[str] = None
    height: Optional[str] = None
    weight: Optional[str] = None
    reach: Optional[str] = None
    stance: Optional[str] = None

    def is_valid(self) -> bool:
        if any(getattr(self, field) == "" for field in ["link", "lastName"]):
            return False

        if any(getattr(self, field) < 0 for field in ["wins", "losses", "draws"]):
            return False

        for field in ["firstName", "nickname"]:
            val = getattr(self, field)
            if val is not None and val == "":
                return False

        for field in ["height", "weight", "reach", "stance"]:
            valid_func: Callable[[Optional[str]], bool] = locals()["is_valid_" + field]
            if not valid_func(getattr(self, field)):
                return False

        return True

    @classmethod
    def from_dict(cls, d: dict) -> Optional["FighterData1"]:
        fd = cls(**d)
        return fd if fd.is_valid() else None


@dataclass(frozen=True, kw_only=True)
class FighterData2:
    fullName: str
    wins: int
    losses: int
    draws: int
    noContests: int
    nickname: Optional[str] = None
    height: Optional[str] = None
    weight: Optional[str] = None
    reach: Optional[str] = None
    stance: Optional[str] = None
    dateOfBirth: Optional[str] = None
    slpm: Optional[float] = None
    strAcc: Optional[int] = None
    sapm: Optional[float] = None
    strDef: Optional[int] = None
    tdAvg: Optional[float] = None
    tdAcc: Optional[int] = None
    tdDef: Optional[int] = None
    subAvg: Optional[float] = None

    def is_valid(self) -> bool:
        if self.fullName == "":
            return False

        if any(getattr(self, field) < 0 for field in ["wins", "losses", "draws", "noContests"]):
            return False

        if self.nickname is not None and self.nickname == "":
            return False

        for field in ["height", "weight", "reach", "stance"]:
            valid_func: Callable[[Optional[str]], bool] = locals()["is_valid_" + field]
            if not valid_func(getattr(self, field)):
                return False

        return all(getattr(self, field) is None for field in STATS_FIELDS) or all(
            isinstance(getattr(self, field), int | float) and getattr(self, field) >= 0
            for field in STATS_FIELDS
        )

    @classmethod
    def from_dict(cls, d: dict) -> Optional["FighterData2"]:
        fd = cls(**d)
        return fd if fd.is_valid() else None


def valid_common_fields(fd1: FighterData1, fd2: FighterData2) -> bool:
    if any(getattr(fd1, field) != getattr(fd2, field) for field in COMMON_FIELDS):
        return False

    # check if name is consistent
    first = fd1.firstName
    last = fd1.lastName
    full = fd2.fullName
    if (first is None and last != full) or (first is not None and first + " " + last != full):
        return False

    # check if reach is consistent
    if type(fd1.reach) != type(fd2.reach):
        return False
    elif isinstance(fd1.reach, str):
        try:
            reach_1 = float(fd1.reach.rstrip('"'))
            reach_2 = float(cast(str, fd2.reach).rstrip('"'))
        except (AttributeError, ValueError):
            return False
        if reach_1 != reach_2:
            return False

    return True


@dataclass(frozen=True, kw_only=True)
class FighterData:
    lastName: str
    wins: int
    losses: int
    draws: int
    noContests: int
    currentChampion: bool
    firstName: Optional[str] = None
    nickname: Optional[str] = None
    height: Optional[str] = None
    weight: Optional[str] = None
    reach: Optional[str] = None
    stance: Optional[str] = None
    dateOfBirth: Optional[str] = None
    slpm: Optional[float] = None
    strAcc: Optional[int] = None
    sapm: Optional[float] = None
    strDef: Optional[int] = None
    tdAvg: Optional[float] = None
    tdAcc: Optional[int] = None
    tdDef: Optional[int] = None
    subAvg: Optional[float] = None

    @classmethod
    def from_parts(cls, fd1: Optional[FighterData1], fd2: Optional[FighterData2]) -> Optional["FighterData"]:
        if fd1 is None or fd2 is None or not valid_common_fields(fd1, fd2):
            return

        data_dict = {}

        fields_1 = chain(["firstName", "lastName", "reach", "currentChampion"], COMMON_FIELDS)
        data_dict.update(
            {field: getattr(fd1, field) for field in fields_1 if getattr(fd1, field) is not None}
        )

        fields_2 = chain(["noContests", "dateOfBirth"], STATS_FIELDS)
        data_dict.update(
            {field: getattr(fd2, field) for field in fields_2 if getattr(fd2, field) is not None}
        )

        return cls(**data_dict)

    def get_name(self) -> dict[str, str]:
        return {
            field: getattr(self, field)
            for field in ["firstName", "lastName", "nickname"]
            if getattr(self, field) is not None
        }

    name = property(fget=get_name)

    def get_record(self) -> dict[str, int]:
        return {field: getattr(self, field) for field in ["wins", "losses", "draws", "noContests"]}

    record = property(fget=get_record)

    def get_height(self) -> Optional[int]:
        if self.height is None:
            return

        pattern = r"(\d{1})' (\d{1,2})\""
        match = re.match(pattern, self.height)
        match = cast(re.Match, match)

        feet = int(match.group(1))
        inches = int(match.group(2))

        height_in = feet * 12 + inches
        return height_in

    heightIn = property(fget=get_height)

    def get_weight(self) -> Optional[int]:
        if self.weight is None:
            return

        pattern = r"(\d+) lbs"
        match = re.match(pattern, self.weight)
        match = cast(re.Match, match)

        weight_lbs = int(match.group(1))
        return weight_lbs

    weightLbs = property(fget=get_weight)

    def get_reach(self) -> Optional[int | float]:
        if self.reach is None:
            return

        pattern = r"(\d+)([.]\d+)?\""
        match = re.match(pattern, self.reach)
        match = cast(re.Match, match)

        i_pt = int(match.group(1))
        if match.group(2) is None:
            return i_pt

        f_pt = float("0" + match.group(2))
        return i_pt + f_pt

    reachIn = property(fget=get_reach)

    def get_physical_features(self) -> Optional[dict[str, int | float]]:
        keys = ["height", "weight", "reach"]
        fields = ["heightIn", "weightLbs", "reachIn"]
        data_dict = {k: getattr(self, f) for k, f in zip(keys, fields) if getattr(self, f) is not None}
        return data_dict if len(data_dict) > 0 else None

    physicalFeatures = property(fget=get_physical_features)

    def get_career_stats(self) -> Optional[dict[str, float]]:
        data_dict = {}

        for field in STATS_FIELDS:
            val = getattr(self, field)
            if val is None:
                continue
            elif isinstance(val, int):
                # Integer values are percentages. They'll be converted into ratios.
                data_dict[field] = val / 100
            elif isinstance(val, float):
                data_dict[field] = val

        return data_dict if len(data_dict) > 0 else None

    careerStats = property(fget=get_career_stats)

    def to_dict(self) -> dict:
        data_dict = {}

        for field in FINAL_FIELDS:
            val = getattr(self, field)
            if val is None:
                continue
            data_dict[field] = val

        return data_dict


def read_fighter_data(
    type_: int,
    first_letter: str,
) -> list[Optional[FighterData1]] | list[Optional[FighterData2]] | None:
    if not (type_ == 1 or type_ == 2):
        return

    if not (first_letter.isalpha() and len(first_letter) == 1):
        return

    data_dir = DATA_DIR_1 if type_ == 1 else DATA_DIR_2
    if not (data_dir.exists() and data_dir.is_dir() and os.access(data_dir, os.R_OK)):
        return

    in_file = data_dir / f"{first_letter}.json"
    if not (in_file.exists() and in_file.is_file() and os.access(in_file, os.R_OK)):
        return

    fighter_class = FighterData1 if type_ == 1 else FighterData2
    with open(in_file, mode="r") as json_file:
        return json.load(json_file, object_hook=lambda d: fighter_class.from_dict(d))


def read_all_data() -> tuple[list[Optional[FighterData1]], list[Optional[FighterData2]]] | None:
    print("READING ALL FIGHTER DATA", end="\n\n")

    list_1 = []
    list_2 = []

    for first_letter in ascii_lowercase:
        print(f"Reading data corresponding to letter {first_letter.upper()}...", end=" ")

        l1 = read_fighter_data(1, first_letter)
        l2 = read_fighter_data(2, first_letter)

        if l1 is None or l2 is None or len(l1) != len(l2):
            print("Failed!")
            continue

        list_1.extend(l1)
        list_2.extend(l2)
        print("Success!")

    if len(list_1) == 0 and len(list_2) == 0:
        return
    return list_1, list_2


def combine_all_data() -> ExitCode:
    data = read_all_data()

    if data is None:
        print("\nFailed to read fighter data!")
        return ExitCode.ERROR

    list_1, list_2 = data
    assert len(list_1) == len(list_2)

    print("\nCOMBINING ALL FIGHTER DATA", end="\n\n")

    combined = []
    num_failed = 0

    for fd1, fd2 in tqdm(zip(list_1, list_2)):
        fd = FighterData.from_parts(fd1, fd2)
        if fd is None:
            num_failed += 1
            continue
        combined.append(fd.to_dict())

    total_fighters = len(list_1)
    if total_fighters == num_failed:
        print("\nComplete failure! No fighter data was combined.")
        return ExitCode.ERROR

    if num_failed > 0:
        print(f"\nPartial success! Failed to combine data for {num_failed} out of {total_fighters} fighters.")
    else:
        print(f"\nComplete success! Combined data for {total_fighters} fighters.")

    print("Saving combined data to JSON...", end=" ")

    if not (DATA_DIR.exists() and DATA_DIR.is_dir() and os.access(DATA_DIR, os.W_OK)):
        print("Failed!")
        return ExitCode.ERROR

    out_file = DATA_DIR / "fighters.json"
    with open(out_file, mode="w") as json_file:
        json.dump(combined, json_file, indent=2)
    print("Done!")

    return ExitCode.PARTIAL_SUCCESS if num_failed > 0 else ExitCode.SUCCESS


if __name__ == "__main__":
    code = combine_all_data()
    exit(code.value)

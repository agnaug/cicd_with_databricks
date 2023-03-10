# "Standard Library"
from typing import Protocol


class DButils(Protocol):
    @property
    def notebook(self):
        pass


def get_username(dbutils: DButils) -> str:
    username = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .userName()
        .get()
        .replace(".", "_")
    )

    return username


def get_user(username: str) -> str:
    return username[: username.index("@")]

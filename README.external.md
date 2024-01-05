# Plugins

This document describes how to create plugins for Lutra.

> **Note**: Lutra is a work in progress, so expect non-backwards-compatible
> changes as we continue to develop.

# Quick Start

1. `Plugins` -> `Create New Plugin`.
2. Edit the plugin name to be "Hello World".
3. Enter the following in the `Code` editor:

```python
def hello_world() -> str:
    return "Hello, world!"
```

4. Press the `Save` button.
5. `Workflows` -> `Create New Workflow` -> `Workflow`.
6. Press the `+` button next to the `Hello World` tool.
7. Enter the following into the `Workflow Description` editor:

```
Say hello to the world.
```

8. Press the `Generate Workflow` button.
9. Press the `Build & Run` button.
10. See Lutra use the action you defined in your plugin!

# Terminology

## Action

An "action" is a unit of functionality that can be used by a Lutra workflow.
Actions usually perform some kind of API call or other external operation. For
example, here are some common actions:

- `get_website_content`
- `gmail_search`
- `google_sheets_append`
- `linkedin_get_profile`

## Plugin

A "plugin" is body of Python 3 code that defines actions. An "action function"
is a Python function in the plugin that implements an action.

# Going Further

## HTTP Requests

In the `Quick Start` section, we created a minimal plugin: a single action
implemented by a single pure function.

Let's create another plugin that does a little more. We'll use a [free API
provided by The Library of
Congress](https://chroniclingamerica.loc.gov/about/api/) as an example API with
which to interact. This API, "Chronicling America", provides information about
historic newspapers.

Create a new plugin, and name it "Chronicling America".

Create an action that returns the results of a title search. Enter the
following into the `Code` editor:

```python
from typing import Any

import httpx


async def chronicling_america_title_search(terms: str) -> Any:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://chroniclingamerica.loc.gov/search/titles/results/",
            params={"terms": terms, "format": "json"},
        )
        return response.raise_for_status().json()
```

`chronicling_america_title_search` is our action function.

Now create a workflow that will use this plugin and action.

1. `Workflows` -> `Create New Workflow` -> `Workflow`.
2. Press the `+` button next to the `Chronicling America` tool.
3. Enter the following into the `Workflow Description` editor:

```
Make a table of Chronicling America newspaper titles matching the term
"michigan".  The table should have a column with the ID and title.
```

> **Tip**: Keep a browser window open with your plugin and a workflow open, as
> we'll be updating both as we go.

If you run build and run this workflow, you will likely see that Lutra is
unable to extract the correct fields, as it doesn't yet understand the
structure of the title search result. We can help Lutra understand.

## Structuring Data

We use Python types and dataclasses to express this structure:

```python
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass
class Newspaper:
    id: str
    publisher: str
    title: str


async def chronicling_america_title_search(terms: str) -> list[Newspaper]:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://chroniclingamerica.loc.gov/search/titles/results/",
            params={"terms": terms, "format": "json"},
        )
    data = response.raise_for_status().json()
    return [
        Newspaper(
            id=item["id"],
            publisher=item["publisher"],
            title=item["title"],
        )
        for item in data["items"]
    ]
```

Enter this code, and save the plugin. Go to your workflow and press the
`Update Workflow` so that it can use the new type definitions.

If you `Build & Run` the workflow, Lutra is now able to extract the correct
fields from the title search result.

## Multiple Actions

A plugin can define multiple actions. Let's add another action to our plugin by
adding the following code which performs a newspaper page search:

```python
@dataclass
class Page:
    id: str
    newspaper_title: str
    text: str


async def chronicling_america_page_search(query: str) -> list[Page]:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://chroniclingamerica.loc.gov/search/pages/results/",
            params={"andtext": query, "format": "json"},
        )
    data = response.raise_for_status().json()
    return [
        Page(
            id=item["id"],
            newspaper_title=item["title"],
            text=item["ocr_eng"],
        )
        for item in data["items"]
    ]
```

Save the plugin.

Now create a workflow that will use this action.

1. `Workflows` -> `Create New Workflow` -> `Workflow`.
2. Press the `+` button next to the `Chronicling America` tool.
3. Enter the following into the `Workflow Description` editor:

```
Make a list of Chronicling America newspaper pages matching the term "economy".
Make a table of those pages with columns for the ID, newspaper title, and a
summary of the page text. The table must have at most 10 rows.
```

# Details

Currently, there are a number of constraints on and quirks of plugin Python
code.

## Exported Actions

Each function that does not begin with an underscore, `_`, defines an action.
Functions that begin with an underscore will not be used (directly) by Lutra.

## <a name="types"/>Types

Actions expose types in these ways:

- Action function parameter types.
- Action function return types.
- Dataclass field values.

These types are limited to the following:

- `bool`
- `int`
- `float`
- `complex`
- `str`
- `set` (of other types in this list)
- `list` (of other types in this list)
- `dict` (of other types in this list)
- `datetime` (imported as `from datetime import datetime`)
- `Optional` (imported as `from typing import Optional`)
- `Any` (imported as `from typing import Any`)
- Dataclasses with fields of the types above. See [Dataclasses](#dataclasses).

An action function return type may also be `None`.

Note that `Any` is mostly provided as an escape hatch from otherwise more
precise typing. Use it sparingly, if at all. More precise types allow Lutra to
better interpret and use your data.

Also note that these constraints do not apply to non-action functions and types
that are not exposed by action functions.

## <a name="dataclasses"/>Dataclasses

If you use dataclasses, always import the `dataclass` decorator as:

```python
from dataclasses import dataclass
```

In addition to the types described in the [Types](#types) section `dataclass`
fields may also refer to previously defined dataclasses, e.g. the following is
valid:

```python
@dataclass
class Foo:
    pass


@dataclass
class Bar:
    foo: Foo
```

TODO: Dataclasses can't actually refer to other dataclasses right now, but I am
about to make a PR that supports this feature.

However, recursive dataclass definitions are disallowed, e.g.:

```python
@dataclass
class Cons:
    car: int
    cdr: Cons # Disallowed!
```

## Docstrings

Action function docstrings can be used to help guide Lutra to use your action
as intended, e.g.:

```python
def max_int(is: list[int]) -> int:
    """
    Return the maximum integer in the given list.

    If the list is empty, raise a ValueError exception.
    """
    if not is:
        raise ValueError("empty list")
    return max(is)
```

See https://peps.python.org/pep-0257/ for more information about docstrings,
particularly the sections about multi-line docstrings for functions.

## Async

Action functions may be synchronous or asynchronous, i.e. both `def` and
`async def` are allowed.

## Available Libraries

The plugin code will run in an environment with access to the following Python
modules, in addition to the Python 3 standard library.

```
bs4
chardet
dateutil
html2text
httpx
jdcal
lxml
magic
numpy
olefile
openpyxl
pandas
pdf2image
pdfminer
pydantic
pypandoc
pytz
rfc3986
soupsieve
sqlite3
tabulate
tenacity
tzdata
ulid
zstandard
```

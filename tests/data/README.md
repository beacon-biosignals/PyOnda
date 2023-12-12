# UUID arrow files

We generate simple tables with Julia to have an example of a UUID4 value and a UUD5 value.
Reading these UUIDs is tested under `tests/test_uuid_read.py` so if you recreate these files make sure you
update the expected_uuid variables in the tests.

```julia
# Snippet to generate uuid4.arrow
using Arrow, UUIDs
row = (; col1=uuid4())
table = [row]
Arrow.write("/path/to/tests/data/uuid4.arrow", table)
```

```julia
# Snippet to generate uuid5.arrow
using Arrow, UUIDs
namespace = uuid4()
row = (; col1=uuid5(namespace, "test"))
table = [row]
Arrow.write("/path/to/tests/data/uuid5.arrow", table)
```
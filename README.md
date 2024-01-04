# PyOnda
Python tools to handle [Onda](https://github.com/beacon-biosignals/Onda.jl/tree/main) formatted data

## Setup
The packaging uses [poetry](https://python-poetry.org/docs/). Follow installation instruction on the official website.
Once poetry is installed, run : 

```shell
# Clone the repository and cd into it
cd PyOnda

# Install dependencies
poetry install
```

Before using s3 related functions make sur to setup the correct AWS_PROFILE
```shell
export AWS_PROFILE=relevant_profile
```

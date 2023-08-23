# PyOnda
Python tools to handle Onda formatted data

## Setup
The packaging uses hatch.

```shell
# Clone the repository and cd into it
cd PyOnda

# Install hatch if not already installed
pip install hatch

# With hatch, create environments
hatch env create
```

Before using s3 related functions make sur to setup the correct AWS_PROFILE
```shell
export AWS_PROFILE=relevant_profile
```
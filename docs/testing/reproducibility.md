# Step33 reproducibility evidence

`make reproducibility` runs the complete Linux SSH authentication-burst E2E twice
from clean Compose state and requires identical **semantic evidence**.

The gate records the dataset/rule content hashes, dependency lock hashes,
container image lock digest, migration versions, contract version, Git commit,
and a semantic E2E fingerprint.

CERBERO classifies this gate as **environment-reproducible / semantic**. It does
not claim bit-for-bit reproducible artifacts. Runtime UUIDv7 values, build
timestamps, and normalization processing timestamps are deliberately excluded
from semantic equivalence.

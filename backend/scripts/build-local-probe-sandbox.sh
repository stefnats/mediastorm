#!/usr/bin/env bash
set -euo pipefail

# Explicit local build only. No downloads, runtime compilation or installation.
if [[ $# -ne 2 || "$(uname -s)" != Linux || "$(uname -m)" != x86_64 ]]; then
  echo 'Usage: build-local-probe-sandbox.sh ABSOLUTE_OUTPUT ABSOLUTE_FFPROBE' >&2
  exit 64
fi
probe_output=$1
probe_binary=$2
if [[ $probe_output != /* || $probe_binary != /* || ! -f $probe_binary || ! -x $probe_binary ]]; then
  echo 'Absolute output and an existing executable ffprobe are required.' >&2
  exit 64
fi
probe_source=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../tools/local-probe-sandbox" && pwd)/launcher.c
umask 077
/usr/bin/gcc -std=c11 -O2 -Wall -Wextra -Werror -fstack-protector-strong -D_FORTIFY_SOURCE=2 -Wl,-z,relro,-z,now -o "$probe_output" "$probe_source"
chmod 0700 "$probe_output"
python3 - "$probe_output" "$probe_binary" "$probe_source" <<'PY'
from pathlib import Path
import hashlib,json,sys
helper,probe,source=map(Path,sys.argv[1:])
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
manifest={'abi':'mediastorm-local-probe/1','helperPath':str(helper),'helperSHA256':sha(helper),'ffprobePath':str(probe),'ffprobeSHA256':sha(probe),'sourceSHA256':sha(source),'compilerPath':'/usr/bin/gcc','compilerSHA256':sha(Path('/usr/bin/gcc').resolve()),'inputCapBytes':8<<30,'platform':'linux/amd64','installed':False}
Path(str(helper)+'.json').write_text(json.dumps(manifest,indent=2)+'\n')
PY

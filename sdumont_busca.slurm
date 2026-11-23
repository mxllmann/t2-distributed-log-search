#!/bin/bash
#
# Compara, no SDumont, a busca distribuida em modo static e dynamic:
#   - dados uniformes:     96 arquivos do mesmo tamanho
#   - dados heterogeneos:  8 arquivos enormes + 88 pequenos
# variando o numero de processos MPI (1, 2, 4, ... workers + o mestre).
#
# Uso (na raiz do projeto, dentro do cluster; de preferencia em $SCRATCH):
#   sbatch sdumont_busca.slurm
#
# Opcional:
#   sbatch --nodes=2 sdumont_busca.slurm                      (2 nos, 96 processos)
#   sbatch --export=ALL,REPS=7,WORD="T23:59:5" sdumont_busca.slurm
#   sbatch --export=ALL,MPI_MODULE=openmpi/4.1.6 sdumont_busca.slurm
#
# WORD padrao e rara (~1 linha em 8640). Palavras comuns (ERROR, INFO, banco...)
# geram milhoes de ocorrencias nesses dados e estouram o int do MPI_Send.
#
#SBATCH --job-name=busca-logs
#SBATCH --partition=sequana_cpu_dev
##SBATCH --account=NOME_DO_PROJETO
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=48
#SBATCH --exclusive
#SBATCH --time=00:20:00
#SBATCH --output=busca_%j.out

set -e
cd "${SLURM_SUBMIT_DIR:-.}"

export MPI_MODULE=${MPI_MODULE:-openmpi}
module purge >/dev/null 2>&1 || true
module load gcc >/dev/null 2>&1 || echo "(aviso: 'module load gcc' falhou, usando o gcc do sistema)"
module load "$MPI_MODULE" >/dev/null 2>&1 || echo "(aviso: 'module load $MPI_MODULE' falhou)"
command -v python3 >/dev/null || module load python >/dev/null 2>&1 || true

if ! command -v mpicc >/dev/null; then
    echo "ERRO: mpicc nao encontrado. Modulos de MPI disponiveis:"
    module avail 2>&1 | grep -i mpi || true
    echo "Rode de novo com: sbatch --export=ALL,MPI_MODULE=<nome> sdumont_busca.slurm"
    exit 1
fi

export NTASKS=${NTASKS:-${SLURM_NTASKS:-4}}
export REPS=${REPS:-5}
export WORD=${WORD:-"T23:59:5"}
export MPIRUN=${MPIRUN:-"mpirun -np"}

echo "=================================================================="
echo " Nos        : ${SLURM_JOB_NODELIST:-$(hostname)}   Job: ${SLURM_JOB_ID:-local}"
echo " Processos  : $NTASKS (1 mestre + $((NTASKS - 1)) workers)"
echo " CPU        : $(lscpu | grep 'Model name' | sed 's/.*: *//')"
echo " Compilador : $(mpicc --version | head -1)"
echo " MPI        : $(mpirun --version 2>&1 | head -1)"
echo " Busca      : \"$WORD\"   (mediana de $REPS execucoes)"
echo " Data       : $(date)"
echo "=================================================================="

# ---------------------------------------------------------------- compilacao
export B=build_busca
rm -rf "$B" && mkdir -p "$B"
gcc -O3 gerador_logs_t2.c -o "$B/gerador_logs_t2"
mpicc -O3 log_hunter.c -o "$B/busca_logs"
echo "Compilado: gerador_logs_t2, busca_logs"

# ---------------------------------------------------------------- dados
export D=dados_busca
rm -rf "$D" && mkdir -p "$D"
"$B/gerador_logs_t2" 96 200000 8 42 "$D/uniforme"
"$B/gerador_logs_t2" 8 2000000 8 7 "$D/grandes"
"$B/gerador_logs_t2" 88 20000 8 9 "$D/pequenos"
mkdir -p "$D/heterogeneo"
for f in "$D"/grandes/*.log; do mv "$f" "$D/heterogeneo/a_${f##*/}"; done
for f in "$D"/pequenos/*.log; do mv "$f" "$D/heterogeneo/b_${f##*/}"; done
rm -rf "$D/grandes" "$D/pequenos"
echo "Dados: $(du -sh "$D/uniforme" | cut -f1) uniforme, $(du -sh "$D/heterogeneo" | cut -f1) heterogeneo"

python3 - <<'EOF'
import os, re, statistics, subprocess

B = os.environ["B"]
D = os.environ["D"]
NTASKS = int(os.environ["NTASKS"])
REPS = int(os.environ["REPS"])
WORD = os.environ["WORD"]
MPIRUN = os.environ["MPIRUN"].split()

DADOS = [("Uniforme (96 x 200.000 linhas)", f"{D}/uniforme"),
         ("Heterogeneo (8 x 2.000.000 + 88 x 20.000 linhas)", f"{D}/heterogeneo")]
MODOS = ["static", "dynamic"]

def executa(procs, pasta, modo):
    cmd = MPIRUN + [str(procs), f"{B}/busca_logs", "--dir", pasta, "--search", WORD, "--mode", modo]
    return subprocess.run(cmd, stdout=subprocess.PIPE, text=True, check=True).stdout

def tempo(saida):
    return float(re.search(r"Tempo de execucao: ([0-9.]+)", saida).group(1))

def mediana(procs, pasta, modo):
    return statistics.median(tempo(executa(procs, pasta, modo)) for _ in range(REPS))

def sem_tempo(saida):
    return [l for l in saida.splitlines() if not l.startswith("Tempo")]

workers, w = [], 1
while w < NTASKS - 1:
    workers.append(w); w *= 2
workers.append(NTASKS - 1)

# ------------------------------------------------------------ 1) conferencia
print(f"\n### 1) Conferencia com {NTASKS} processos (static x dynamic x grep)")
for nome, pasta in DADOS:
    s = executa(NTASKS, pasta, "static")
    d = executa(NTASKS, pasta, "dynamic")
    total = int(re.search(r"Total de ocorrencias: (\d+)", d).group(1))
    arquivos = sorted(os.path.join(pasta, f) for f in os.listdir(pasta))
    ref = subprocess.run(["grep", "-c", "-F", "-h", "--", WORD] + arquivos, stdout=subprocess.PIPE, text=True).stdout
    ref = sum(int(n) for n in ref.split())
    ok = sem_tempo(s) == sem_tempo(d) and total == ref
    print(f"  {nome}: {total} ocorrencias, grep = {ref} -> {'OK' if ok else 'DIFERENCA ENCONTRADA'}")

# ------------------------------------------------------------ 2) escalabilidade
for nome, pasta in DADOS:
    print(f"\n### 2) Escalabilidade forte - {nome}")
    print(f"{'procs':>6}{'workers':>8}" + "".join(f"{m:>24}" for m in MODOS) + f"{'static/dynamic':>16}")
    print(f"{'':>14}" + "".join(f"{'T(s)    S   efic.':>24}" for _ in MODOS))
    t1 = {}
    for w in workers:
        linha = f"{w + 1:>6}{w:>8}"
        t = {}
        for m in MODOS:
            t[m] = mediana(w + 1, pasta, m)
            t1.setdefault(m, t[m])
            linha += f"{t[m]:>12.4f}{t1[m] / t[m]:>6.1f}{100 * t1[m] / (t[m] * w):>5.0f}%"
        linha += f"{t['static'] / t['dynamic']:>15.2f}x"
        print(linha)

print("\nS = T(1 worker) / T(p) do proprio modo.  efic. = S / workers.")
print("static/dynamic > 1 significa que o dynamic foi mais rapido.")
EOF

rm -rf "$D"
echo
echo "Fim: $(date)"

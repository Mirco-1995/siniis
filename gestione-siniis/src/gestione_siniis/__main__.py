import typer
from gestione_siniis.cli import main as run_main


def main():
    typer.run(run_main)

if __name__ == "__main__":
    main()

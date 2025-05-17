import subprocess
import sys
from src.logger import Logger
from src.utils import parse_args, validate_dates

logger = Logger.get_logger()

def run_script(script, start_date, end_date=None):
    """Ejecuta un script Python como subproceso."""
    command = ["python3", script, "--start", start_date]
    if end_date:
        command += ["--end", end_date]
    Logger.log_job_start(f"Ejecutando script: {script}")
    try:
        subprocess.run(command, check=True)
        Logger.log_job_end(f"✅ Finalizó correctamente: {script}", success=False)
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Error en {script} con código de salida {e.returncode}")
        sys.exit(e.returncode)
    except Exception as ex:
        logger.critical(f"💥 Error inesperado al ejecutar {script}: {str(ex)}", exc_info=True)
        sys.exit(2)

def main():
    args = parse_args("Ejecución de pipeline financiero")

    try:
        start_date, end_date = validate_dates(args.start, args.end)
    except ValueError as e:
        logger.error(f"❌ Error en la validacion de fechas: {e}", exc_info=True)
        raise

    Logger.log_header("Iniciando ejecución secuencial de los jobs")

    run_script("src/jobs/load_wallet_history.py", start_date, end_date)
    run_script("src/jobs/fetch_cdi_rates.py", start_date, end_date)
    run_script("src/jobs/calculate_interest_payments.py", start_date, end_date)

    Logger.log_header("Pipeline completo")

if __name__ == "__main__":    
    main()

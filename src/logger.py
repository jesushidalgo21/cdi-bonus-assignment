import logging
from textwrap import fill
from typing import Optional

class Logger:
    _instance = None
    _logger_initialized = False
    _header_width = 80  # Ancho máximo del encabezado
    _header_char = '='  # Carácter para el borde del encabezado

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._initialize_logger()
        return cls._instance

    @classmethod
    def _initialize_logger(cls):
        if not cls._logger_initialized:
            cls.logger = logging.getLogger("CDIBonusAssignment")
            cls.logger.setLevel(logging.INFO)
            
            if not cls.logger.handlers:
                handler = logging.StreamHandler()
                cls.logger.addHandler(handler)
            
            cls._logger_initialized = True

    @classmethod
    def get_logger(cls):
        if not cls._logger_initialized:
            cls._initialize_logger()
        return cls.logger

    @classmethod
    def log_header(cls, message: str, level: str = 'info', 
                 width: Optional[int] = None, char: Optional[str] = None):
        """
        Crea un encabezado visualmente destacado para marcar secciones importantes.
        
        Args:
            message (str): Texto a mostrar en el encabezado
            level (str): Nivel de logging ('info', 'warning', 'error')
            width (int, optional): Ancho del encabezado. Si no se especifica, usa el valor por defecto.
            char (str, optional): Carácter para el borde. Si no se especifica, usa el valor por defecto.
        """
        width = width or cls._header_width
        char = char or cls._header_char
        
        # Ajustar el mensaje al ancho especificado
        wrapped_msg = fill(message, width=width-4)
        border = char * width
        
        # Construir el encabezado
        header_lines = [
            border,
            f"{char} {wrapped_msg.center(width-4)} {char}",
            border
        ]
        header = "\n".join(header_lines)
        
        # Loggear según el nivel
        logger = cls.get_logger()
        if level.lower() == 'warning':
            logger.warning(header)
        elif level.lower() == 'error':
            logger.error(header)
        else:
            logger.info(header)

    @classmethod
    def log_job_start(cls, job_name: str):
        """Encabezado especial para inicio de jobs"""
        cls.log_header(f" INICIANDO JOB: {job_name} ", level='info')

    @classmethod
    def log_job_end(cls, job_name: str, success: bool = True):
        """Encabezado especial para finalización de jobs"""
        cls.log_header(f" JOB COMPLETADO: {job_name}", level='info')
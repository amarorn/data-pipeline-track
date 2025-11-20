"""
Utilitário para limpeza automática de arquivos temporários
"""
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import logging

from shared.config.settings import settings


class TempFileCleanup:
    """Gerencia limpeza de arquivos temporários"""

    def __init__(self):
        self.logger = logging.getLogger("TempFileCleanup")
        self.temp_dir = settings.LOCAL_TEMP_DIR

    def cleanup_old_files(self, hours: Optional[int] = None):
        """
        Remove arquivos temporários mais antigos que X horas

        Args:
            hours: Tempo de retenção em horas
        """
        if not settings.AUTO_CLEANUP_TEMP:
            self.logger.info("Auto-cleanup desabilitado")
            return

        hours = hours or settings.TEMP_RETENTION_HOURS
        cutoff_time = datetime.now() - timedelta(hours=hours)

        if not self.temp_dir.exists():
            return

        deleted_count = 0
        freed_space = 0

        try:
            for item in self.temp_dir.rglob("*"):
                if item.is_file():
                    file_time = datetime.fromtimestamp(item.stat().st_mtime)

                    if file_time < cutoff_time:
                        try:
                            file_size = item.stat().st_size
                            item.unlink()
                            deleted_count += 1
                            freed_space += file_size
                        except Exception as e:
                            self.logger.warning(f"Erro ao deletar {item}: {e}")

            # Remove diretórios vazios
            self._remove_empty_dirs(self.temp_dir)

            if deleted_count > 0:
                freed_mb = freed_space / (1024 * 1024)
                self.logger.info(
                    f"Cleanup: {deleted_count} arquivos removidos, "
                    f"{freed_mb:.2f} MB liberados"
                )

        except Exception as e:
            self.logger.error(f"Erro no cleanup: {e}")

    def _remove_empty_dirs(self, directory: Path):
        """Remove diretórios vazios recursivamente"""
        for item in directory.iterdir():
            if item.is_dir():
                self._remove_empty_dirs(item)
                try:
                    if not any(item.iterdir()):
                        item.rmdir()
                except Exception:
                    pass

    def get_temp_disk_usage(self) -> dict:
        """Retorna uso de disco do diretório temporário"""
        if not self.temp_dir.exists():
            return {"total_size_mb": 0, "file_count": 0}

        total_size = 0
        file_count = 0

        for item in self.temp_dir.rglob("*"):
            if item.is_file():
                total_size += item.stat().st_size
                file_count += 1

        return {
            "total_size_mb": total_size / (1024 * 1024),
            "file_count": file_count,
            "path": str(self.temp_dir)
        }


def schedule_cleanup_task():
    """
    Função para ser executada periodicamente (ex: cron job)
    """
    cleanup = TempFileCleanup()

    # Log uso atual
    usage = cleanup.get_temp_disk_usage()
    print(f"Uso temp antes: {usage['total_size_mb']:.2f} MB, {usage['file_count']} arquivos")

    # Executa cleanup
    cleanup.cleanup_old_files()

    # Log uso após cleanup
    usage = cleanup.get_temp_disk_usage()
    print(f"Uso temp após: {usage['total_size_mb']:.2f} MB, {usage['file_count']} arquivos")


if __name__ == "__main__":
    schedule_cleanup_task()

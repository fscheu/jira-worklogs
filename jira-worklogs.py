import os
import configparser
import base64
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Union

import pandas as pd
import requests

class JiraClient:
    def __init__(self, config_path: str = "config.ini"):
        # Leer configuración desde config.ini
        config = configparser.ConfigParser()
        config.read(config_path)

        self.JIRA_URL = config["JIRA"]["BASE_URL"].rstrip("/")
        self.PROJECT_KEY = config["JIRA"]["PROJECT"]
        self.API_TOKEN = config["JIRA"]["TOKEN"]
        self.EMAIL = config["JIRA"]["EMAIL"]

        # Configuración de autenticación básica
        auth_string = f"{self.EMAIL}:{self.API_TOKEN}".encode("utf-8")
        base64_auth = base64.b64encode(auth_string).decode("utf-8")

        self.HEADERS = {
            "Authorization": f"Basic {base64_auth}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        self._base_url = f"{self.JIRA_URL}/rest/api/3"
        self._session = requests.Session()
        self._session.headers.update(self.HEADERS)

    def test_connection(self):
        """Prueba la conexión con la API de JIRA."""
        url = f"{self.JIRA_URL}/rest/api/3/myself"
        response = self._session.get(url)
        if response.status_code == 200:
            print("Conexión exitosa con la API.")
            print(f"Usuario: {response.json()['displayName']}")
        else:
            print(f"Error en la conexión: {response.text}")

    def _get_paginated_results(
        self,
        url: str,
        results_key: str,
        parameters: Dict[str, Union[str, int]] = None,
        use_post: bool = False,
    ) -> Generator[Dict[str, Any], None, None]:
        """Obtiene resultados de una llamada paginada que utiliza 'maxResults', 'startAt' y 'total'."""
        parameters = parameters or {}
        results_per_page = 1000
        parameters["maxResults"] = results_per_page
        count_next = 0
        while True:
            parameters["startAt"] = count_next
            if use_post:
                response = self._session.post(url, json=parameters)
            else:
                response = self._session.get(url, params=parameters)
            response.raise_for_status()
            response_json = response.json()
            results = response_json.get(results_key, [])

            if response_json.get("maxResults", results_per_page) < results_per_page:
                # Algunos llamados limitan el valor máximo de maxResults
                results_per_page = response_json.get("maxResults", results_per_page)
                parameters["maxResults"] = results_per_page

            for result in results:
                yield result

            count_next += results_per_page
            if count_next >= response_json.get("total", 0):
                return

    def _get_paginated_results_with_next_page_link(
        self, url: str
    ) -> Generator[Dict[str, Any], None, None]:
        """Obtiene resultados de una llamada que devuelve un payload con los atributos lastPage y nextPage."""
        is_last_page = False
        while not is_last_page:
            response = self._session.get(url)
            response.raise_for_status()
            response_json = response.json()
            for result in response_json.get("values", []):
                yield result

            is_last_page = response_json.get("lastPage", True)
            if not is_last_page:
                url = response_json["nextPage"]

    def retrieve_worklogs_updated_since(self, start: datetime) -> List[Dict[str, Any]]:
        """Recupera objetos de worklog creados o actualizados desde la fecha proporcionada."""
        worklog_ids: List[str] = []
        timestamp = int(start.timestamp() * 1000)
        url = f"{self._base_url}/worklog/updated?since={timestamp}"

        for worklog_entry in self._get_paginated_results_with_next_page_link(url):
            worklog_ids.append(worklog_entry["worklogId"])

        worklogs_per_page = 1000
        ids_in_groups_per_page = [
            worklog_ids[i : i + worklogs_per_page]
            for i in range(0, len(worklog_ids), worklogs_per_page)
        ]
        worklogs_by_id: Dict[str, Dict[str, Any]] = {}

        for ids_to_get in ids_in_groups_per_page:
            response = self._session.post(
                f"{self._base_url}/worklog/list", json={"ids": ids_to_get}
            )
            response.raise_for_status()
            worklogs = response.json()
            for worklog in worklogs:
                worklogs_by_id[worklog["id"]] = worklog

        return list(worklogs_by_id.values())

    def search_issues(self, jql: str, fields: List[str] = None) -> List[Dict[str, Any]]:
        """Retorna issues que coinciden con una consulta JQL especificada."""
        issues: List[Dict[str, Any]] = []
        parameters: Dict[str, Union[str, List[str]]] = {"jql": jql}
        if fields:
            parameters["fields"] = fields
        search_url = f"{self._base_url}/search"

        for issue in self._get_paginated_results(
            search_url,
            results_key="issues",
            parameters=parameters,
            use_post=True,
        ):
            issues.append(issue)

        return issues

    def retrieve_issues_for_worklogs(
        self, worklogs: List[Dict[str, Any]], fields: List[str] = None
    ) -> List[Dict[str, Any]]:
        """Obtiene objetos de Issue referenciados en una lista de worklogs."""
        issue_ids = {worklog['issueId'] for worklog in worklogs}
        jql = f"id in ({','.join(str(issue_id) for issue_id in issue_ids)})"
        return self.search_issues(jql, fields=fields)

    def retrieve_worklogs_in_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """
        Recupera todos los worklogs cargados en JIRA para un rango de fechas determinado.

        :param start_date: Fecha de inicio
        :param end_date: Fecha de fin
        :return: Lista de worklogs
        """
        # Recuperar worklogs actualizados desde start_date
        all_worklogs = self.retrieve_worklogs_updated_since(start_date)

        # Filtrar worklogs dentro del rango de fechas
        filtered_worklogs = [
            worklog for worklog in all_worklogs
            if start_date <= datetime.strptime(worklog['started'][:10], '%Y-%m-%d') <= end_date
        ]

        return filtered_worklogs

    def load_epicas_mapping(self, excel_path: str) -> None:
        """
        Carga el archivo Excel que contiene el mapeo de Epicas a Proyecto Económico.

        :param excel_path: Ruta al archivo Excel
        :return: DataFrame con el mapeo
        """
        self.epicas_df = pd.read_excel(excel_path, sheet_name="Sheet1")


# **Ejemplo de Uso**
if __name__ == "__main__":
    client = JiraClient("config.ini")
    client.test_connection()

    # Recuperar worklogs de los últimos 14 días
    # recent_worklogs = client.retrieve_worklogs_updated_since(
    #     datetime.now() - timedelta(days=14)
    # )
    # print(f"Cantidad de worklogs recuperados: {len(recent_worklogs)}")

    # # Recuperar issues asociadas a los worklogs
    # issues_for_worklogs = client.retrieve_issues_for_worklogs(recent_worklogs, fields=["key", "project", "summary", "parent"])
    # print(f"Cantidad de issues recuperadas: {len(issues_for_worklogs)}")

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 30)

    worklogs_in_range = client.retrieve_worklogs_in_date_range(start_date, end_date)
    print(f"Cantidad de worklogs en el rango: {len(worklogs_in_range)}")

    # Recuperar issues asociadas a los worklogs
    issues_for_worklogs = client.retrieve_issues_for_worklogs(worklogs_in_range, fields=["key", "project", "summary", "parent"])
    print(f"Cantidad de issues recuperadas: {len(issues_for_worklogs)}")
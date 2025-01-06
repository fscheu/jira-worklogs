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

        # Cargar mapeo desde Excel
        excel_mappings = self.load_excel_mappings(config["XLS"]["MAPPING_FILE"])
        self.recursos_df = excel_mappings["Recursos"]
        self.tareas_df = excel_mappings["Tareas"]
        self.epicas_df = excel_mappings["Epicas"]

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

    def load_excel_mappings(self, excel_path: str) -> Dict[str, pd.DataFrame]:
        """
        Carga las tres hojas del archivo Excel en DataFrames de pandas.

        :param excel_path: Ruta al archivo Excel.
        :return: Diccionario con DataFrames de cada hoja.
        """
        try:
            xls = pd.ExcelFile(excel_path)
            recursos_df = pd.read_excel(xls, sheet_name="Recursos")
            tareas_df = pd.read_excel(xls, sheet_name="Tareas")
            epicas_df = pd.read_excel(xls, sheet_name="Epicas")
            return {
                "Recursos": recursos_df,
                "Tareas": tareas_df,
                "Epicas": epicas_df
            }
        except Exception as e:
            print(f"Error al cargar el archivo Excel: {e}")
            raise


    def filter_worklogs_by_recursos(self, worklogs_df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra los worklogs para incluir solo aquellos generados por los recursos listados.

        :param worklogs_df: DataFrame de worklogs.
        :param recursos_df: DataFrame de recursos.
        :return: DataFrame filtrado de worklogs.
        """
        recursos_list = self.recursos_df['Recurso'].dropna().unique().tolist()
        filtered_worklogs = worklogs_df[worklogs_df['author_displayName'].isin(recursos_list)]
        return filtered_worklogs


    def assign_proyecto_economico(self, worklogs_df: pd.DataFrame, issues_df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
        """
        Asigna el Proyecto Económico a cada worklog basándose en Tareas y Epicas.

        :param worklogs_df: DataFrame de worklogs filtrados.
        :param issues_df: DataFrame de issues con detalles.
        :param tareas_df: DataFrame de mapeo de Tareas.
        :param epicas_df: DataFrame de mapeo de Epicas.
        :return: Tuple de DataFrames (cruzados, no_cruzados).
        """
        # Crear DataFrame de issues con campos necesarios
        issues_filtered = issues_df[['id', 'key', 'parent']].copy()
        issues_filtered.rename(columns={'id': 'issue_Id', 'key': 'issue_key', 'parent': 'parent_key'}, inplace=True)

        # Merge worklogs con issues para obtener parent_key
        merged_df = worklogs_df.merge(issues_filtered, left_on='issueId', right_on='issue_Id', how='left')

        # Asignar Proyecto_Economico según Tareas
        merged_df = merged_df.merge(self.tareas_df, left_on='issue_key', right_on='KEY', how='left', suffixes=('', '_tareas'))
        merged_df.rename(columns={'Proyecto_Economico': 'Proyecto_Economico_Tareas'}, inplace=True)

        # Asignar Proyecto_Economico según Epicas donde Tareas no asignaron
        epicas_mapping = self.epicas_df.set_index('Parent_Key')['Proyecto_Economico'].to_dict()
        merged_df['Proyecto_Economico_Epicas'] = merged_df['parent_key'].map(epicas_mapping)

        # Determinar Proyecto_Economico con precedencia a Tareas
        merged_df['Proyecto_Economico'] = merged_df['Proyecto_Economico_Tareas'].combine_first(merged_df['Proyecto_Economico_Epicas'])

        # Separar en cruzados y no cruzados
        cruzados_df = merged_df[merged_df['Proyecto_Economico'].notna()].copy()
        no_cruzados_df = merged_df[merged_df['Proyecto_Economico'].isna()].copy()

        return cruzados_df, no_cruzados_df


    def worklogs_to_dataframe(self, worklogs: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Convierte una lista de worklogs a un DataFrame de pandas.

        :param worklogs: Lista de diccionarios de worklogs.
        :return: DataFrame de worklogs.
        """
        # Normalizar la estructura anidada de worklogs
        worklogs_normalized = pd.json_normalize(worklogs)
        worklogs_normalized = worklogs_normalized[['id', 'issueId', 'author.displayName', 'started', 'timeSpentSeconds', 'updated']]
        # Renombrar columnas para facilitar el acceso
        worklogs_normalized.rename(columns={
            'author.displayName': 'author_displayName',
            'started': 'started',
            'timeSpentSeconds': 'timeSpentSeconds',
            # Agrega otros campos según sea necesario
        }, inplace=True)
        return worklogs_normalized

    def issues_to_dataframe(self, issues: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Convierte una lista de issues a un DataFrame de pandas.

        :param issues: Lista de diccionarios de issues.
        :return: DataFrame de issues.
        """
        issues_normalized = pd.json_normalize(issues)
        # Renombrar columnas para facilitar el acceso
        issues_normalized.rename(columns={
            'key': 'key',
            'fields.project.name': 'project',
            'fields.summary': 'summary',
            'fields.parent.key': 'parent'
        }, inplace=True)
        return issues_normalized


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

    # Convertir worklogs a DataFrame
    worklogs_df = client.worklogs_to_dataframe(worklogs_in_range)

    # Recuperar issues asociadas a los worklogs
    issues_for_worklogs = client.retrieve_issues_for_worklogs(worklogs_in_range, fields=["key", "project", "summary", "parent"])
    print(f"Cantidad de issues recuperadas: {len(issues_for_worklogs)}")

    # Convertir issues a DataFrame
    issues_df = client.issues_to_dataframe(issues_for_worklogs)

    # Filtrar worklogs por recursos
    filtered_worklogs_df = client.filter_worklogs_by_recursos(worklogs_df)
    print(f"Cantidad de worklogs después de filtrar por recursos: {len(filtered_worklogs_df)}")

    # Asignar Proyecto_Economico
    cruzados_df, no_cruzados_df = client.assign_proyecto_economico(
        filtered_worklogs_df,
        issues_df
    )
    print(f"Worklogs cruzados: {len(cruzados_df)}")
    print(f"Worklogs no cruzados: {len(no_cruzados_df)}")

    def generate_output_files(cruzados: pd.DataFrame, no_cruzados: pd.DataFrame, output_path_cruzados: str, output_path_no_cruzados: str):
        """
        Genera archivos Excel para los worklogs cruzados y no cruzados.

        :param cruzados: DataFrame de worklogs cruzados.
        :param no_cruzados: DataFrame de worklogs no cruzados.
        :param output_path_cruzados: Ruta de salida para worklogs cruzados.
        :param output_path_no_cruzados: Ruta de salida para worklogs no cruzados.
        """
        # Procesar worklogs cruzados
        cruzados_processed = cruzados.copy()
        cruzados_processed.rename(columns={
            "author_displayName": "AD_Usuario",
            "started": "Fecha_Worklog",
            "timeSpentSeconds": "Cantidad_Horas",
            "Proyecto_Economico": "ID_Proyecto"
            # Puedes agregar más renombramientos si es necesario
        }, inplace=True)
        # Convertir segundos a horas
        cruzados_processed['Cantidad_Horas'] = cruzados_processed['Cantidad_Horas'] / 3600
        # Seleccionar columnas necesarias
        cruzados_final = cruzados_processed[['AD_Usuario', 'Fecha_Worklog', 'Cantidad_Horas', 'ID_Proyecto', 'N_Proyecto_Economico']].copy()
        cruzados_final.rename(columns={'N_Proyecto_Economico': 'Nombre_Proyecto'}, inplace=True)

        # Guardar worklogs cruzados a Excel
        cruzados_final.to_excel(output_path_cruzados, index=False)

        # Guardar worklogs no cruzados a Excel
        no_cruzados.to_excel(output_path_no_cruzados, index=False)

    # Generar los archivos de salida
    generate_output_files(
        cruzados_df,
        no_cruzados_df,
        "worklogs_cruzados.xlsx",
        "worklogs_no_cruzados.xlsx"
    )

    print("Archivos generados exitosamente.")
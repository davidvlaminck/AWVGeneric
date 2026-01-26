import logging
import json
import time
from pathlib import Path

import msal
import requests

from settings_loader import load_settings

logging.basicConfig(
    level=logging.INFO,  # Toon INFO en hoger
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")


class OneDriveClient:
    def __init__(
        self,
        client_id: str,
        token_file: Path,
        authority: str = "https://login.microsoftonline.com/consumers",
        scopes=None,
    ):
        self.client_id = client_id
        self.token_file = Path(token_file)
        self.authority = authority
        self.scopes = scopes or ["Files.ReadWrite.All"]
        self.app = msal.PublicClientApplication(self.client_id, authority=self.authority)

    # ---------- Token persistence ----------
    def _load_token(self) -> dict:
        if not self.token_file.exists():
            return {}
        with self.token_file.open("r") as f:
            return json.load(f)

    def _save_token(self, token_data: dict):
        with self.token_file.open("w") as f:
            json.dump(token_data, f, indent=2)

    # ---------- Authentication ----------
    def login_interactive(self):
        """Perform an interactive login and save the token."""
        result = self.app.acquire_token_interactive(scopes=self.scopes)
        if "access_token" not in result:
            raise RuntimeError(f"Login fout: {result.get('error_description')}")
        result["expires_at"] = time.time() + result.get("expires_in", 3600)
        self._save_token(result)
        logging.info(f"Token opgeslagen in {self.token_file.resolve()}")

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing if needed."""
        token_data = self._load_token()
        if not token_data:
            logging.info("Geen token gevonden. Start interactieve login...")
            self.login_interactive()
            token_data = self._load_token()

        expires_at = token_data.get("expires_at", 0)
        if time.time() > expires_at - 60:
            logging.info("Token verlopen of bijna verlopen, verversen...")
            refreshed = self.app.acquire_token_by_refresh_token(
                token_data.get("refresh_token"), self.scopes
            )
            if "access_token" in refreshed:
                refreshed["expires_at"] = time.time() + refreshed.get("expires_in", 3600)
                self._save_token(refreshed)
                token_data = refreshed
            else:
                logging.info("Kon token niet verversen. Nieuwe login vereist.")
                self.login_interactive()
                token_data = self._load_token()
        else:
            logging.info("Bestaande token is nog geldig.")

        return token_data["access_token"]

    # ---------- Graph API calls ----------
    def list_root_files(self):
        """List files/folders in the root of the user's OneDrive."""
        access_token = self.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        url = "https://graph.microsoft.com/v1.0/me/drive/root/children"
        resp = requests.get(url, headers=headers)

        if resp.status_code == 200:
            items = resp.json().get("value", [])
            print("📂 Bestanden in OneDrive-root:")
            for item in items:
                soort = "Map" if "folder" in item else "Bestand"
                print(f"- {item['name']} ({soort})")
        else:
            print(f"Fout {resp.status_code}: {resp.text}")

    def list_folder_files(self):
        """List files/folders in a specific directory of the user's OneDrive."""
        raise NotImplementedError

    def delete_file(self):
        raise NotImplementedError

    def download_file_by_name(self, filename: str, save_path: Path):
        """Zoekt bestand in root en downloadt het lokaal."""
        access_token = self.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}

        # Zoek het bestand in de root
        list_url = "https://graph.microsoft.com/v1.0/me/drive/root/children"
        resp = requests.get(list_url, headers=headers)
        if resp.status_code != 200:
            logging.error(f"Fout bij ophalen lijst: {resp.status_code} - {resp.text}")
            return

        items = resp.json().get("value", [])
        match = next((i for i in items if i["name"].lower() == filename.lower()), None)
        if not match:
            logging.warning(f"Bestand '{filename}' niet gevonden in root")
            return

        file_id = match["id"]
        download_url = f"https://graph.microsoft.com/v1.0/me/drive/items/{file_id}/content"
        download_resp = requests.get(download_url, headers=headers, stream=True)

        if download_resp.status_code == 200:
            with open(save_path, "wb") as f:
                for chunk in download_resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"✅ Bestand '{filename}' gedownload naar {save_path}")
        else:
            logging.error(f"Fout bij downloaden: {download_resp.status_code} - {download_resp.text}")

    def upload_file(self, local_path: Path, onedrive_path: str = None):
        """
        Uploadt een bestand naar OneDrive.
        :param local_path: Lokaal pad naar bestand
        :param onedrive_path: Pad + bestandsnaam in OneDrive (default = root + bestandsnaam)
        """
        access_token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/octet-stream"
        }

        local_path = Path(local_path)
        if not local_path.exists():
            logging.error(f"Lokaal bestand '{local_path}' bestaat niet")
            return

        if not onedrive_path:
            onedrive_path = local_path.name  # zelfde bestandsnaam

        url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{onedrive_path}:/content"
        logging.info(f"⏫ Uploaden van '{local_path}' naar '{onedrive_path}' in OneDrive...")

        with open(local_path, "rb") as f:
            resp = requests.put(url, headers=headers, data=f)

        if resp.status_code in {200, 201}:
            logging.info(f"✅ Bestand succesvol geüpload als '{onedrive_path}'")
        else:
            logging.error(f"Fout bij upload: {resp.status_code} - {resp.text}")


# ---------- Gebruik ----------
if __name__ == "__main__":
    settings = load_settings()

    client = OneDriveClient(
        client_id=settings["azure"]["client_id"],
        token_file=Path(settings.get("files", {}).get("token_file") or "token_onedrive.json"),
    )

    client.list_root_files()

    client.download_file_by_name("settings_loader.py", Path("settings_loader.py"))

    client.upload_file(Path("settings_loader.py"))

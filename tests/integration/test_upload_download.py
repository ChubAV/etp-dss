import uuid

import pytest


@pytest.mark.integration
async def test_full_upload_download_cycle(client, service_token):
    owner_id = str(uuid.uuid4())
    uploaded_by = str(uuid.uuid4())

    # 1. Get upload token
    resp = await client.post(
        "/api/v2/documents/upload-token",
        json={
            "owner_type": "LOT",
            "owner_id": owner_id,
            "visibility": "PRIVATE",
            "file_name": "test.pdf",
            "content_type": "application/pdf",
            "max_size_bytes": 20 * 1024 * 1024,
            "uploaded_by": uploaded_by,
        },
        headers={"Authorization": f"Bearer {service_token}"},
    )
    assert resp.status_code == 201, resp.text
    upload_token = resp.json()["upload_token"]

    # 2. Upload file
    resp = await client.post(
        "/api/v2/documents/upload",
        files={"file": ("test.pdf", b"PDF content here", "application/pdf")},
        headers={"X-Upload-Token": upload_token},
    )
    assert resp.status_code == 201, resp.text
    file_id = resp.json()["file_id"]
    assert resp.json()["uploaded_by"] == uploaded_by

    # 3. Get metadata
    resp = await client.get(
        f"/api/v2/documents/{file_id}",
        headers={"Authorization": f"Bearer {service_token}"},
    )
    assert resp.status_code == 200

    # 4. Get download token
    resp = await client.post(
        "/api/v2/documents/download-token",
        json={"file_id": file_id, "user_id": uploaded_by},
        headers={"Authorization": f"Bearer {service_token}"},
    )
    assert resp.status_code == 201
    download_token = resp.json()["download_token"]

    # 5. Download (redirect)
    resp = await client.get(
        f"/api/v2/documents/download?token={download_token}",
        follow_redirects=False,
    )
    assert resp.status_code == 302

    # 6. Soft delete
    resp = await client.delete(
        f"/api/v2/documents/{file_id}",
        headers={"Authorization": f"Bearer {service_token}"},
    )
    assert resp.status_code == 204

    # 7. Verify deleted
    resp = await client.get(
        f"/api/v2/documents/{file_id}",
        headers={"Authorization": f"Bearer {service_token}"},
    )
    assert resp.status_code == 404

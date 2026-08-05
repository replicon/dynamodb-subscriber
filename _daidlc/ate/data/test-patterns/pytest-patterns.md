# Pytest Test Patterns for D-AIDLC

## File Naming
- `test_*.py` (prefix convention)
- `*_test.py` (suffix convention)
- Located in `tests/` directory mirroring source structure

## Basic Structure
```python
import pytest
from mymodule import function_name

class TestFunctionName:
    def test_valid_input_returns_expected(self):
        result = function_name(valid_input)
        assert result == expected_output

    def test_invalid_input_raises_error(self):
        with pytest.raises(ValueError, match="error message"):
            function_name(invalid_input)

    def test_empty_input_returns_none(self):
        result = function_name("")
        assert result is None
```

## Fixtures
```python
@pytest.fixture
def db_session():
    session = create_test_session()
    yield session
    session.rollback()
    session.close()

@pytest.fixture
def sample_user(db_session):
    user = User(name="Test", email="test@example.com")
    db_session.add(user)
    db_session.commit()
    return user

def test_get_user(db_session, sample_user):
    result = get_user(db_session, sample_user.id)
    assert result.name == "Test"
```

## Parametrize
```python
@pytest.mark.parametrize("input_val,expected", [
    ("valid", True),
    ("", False),
    (None, False),
    ("edge-case", True),
])
def test_validate_input(input_val, expected):
    assert validate(input_val) == expected
```

## Async Testing
```python
import pytest

@pytest.mark.asyncio
async def test_fetch_data():
    result = await fetch_data(test_id)
    assert result["status"] == "success"
```

## API Testing (with httpx/TestClient)
```python
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_get_users():
    response = client.get("/api/users")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_create_user_validation():
    response = client.post("/api/users", json={"name": ""})
    assert response.status_code == 422
```

## Mocking
```python
from unittest.mock import patch, MagicMock

@patch("mymodule.database.query")
def test_service_calls_db(mock_query):
    mock_query.return_value = [{"id": 1}]
    result = service.get_data(1)
    mock_query.assert_called_once_with("SELECT * FROM table WHERE id = %s", (1,))
```

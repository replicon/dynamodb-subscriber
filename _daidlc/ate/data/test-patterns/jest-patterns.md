# Jest Test Patterns for D-AIDLC

## File Naming
- `*.test.js` / `*.test.ts` (co-located with source)
- `__tests__/*.js` / `__tests__/*.ts` (dedicated test directory)

## Basic Structure
```javascript
describe('ModuleName', () => {
  describe('functionName', () => {
    it('should return expected result for valid input', () => {
      const result = functionName(validInput);
      expect(result).toBe(expectedOutput);
    });

    it('should throw for invalid input', () => {
      expect(() => functionName(invalidInput)).toThrow('error message');
    });

    it('should handle edge case: empty input', () => {
      const result = functionName('');
      expect(result).toBeNull();
    });
  });
});
```

## Async Testing
```javascript
it('should fetch data successfully', async () => {
  const result = await fetchData(id);
  expect(result).toEqual(expectedData);
});
```

## Mocking
```javascript
jest.mock('./database');
const { query } = require('./database');

beforeEach(() => {
  query.mockClear();
});

it('should call database with correct params', async () => {
  query.mockResolvedValue(mockResult);
  await service.getData(id);
  expect(query).toHaveBeenCalledWith('SELECT * FROM table WHERE id = $1', [id]);
});
```

## API Testing (with supertest)
```javascript
const request = require('supertest');
const app = require('../app');

describe('GET /api/users', () => {
  it('should return 200 with user list', async () => {
    const res = await request(app).get('/api/users');
    expect(res.status).toBe(200);
    expect(res.body).toBeInstanceOf(Array);
  });

  it('should return 401 without auth', async () => {
    const res = await request(app).get('/api/users');
    expect(res.status).toBe(401);
  });
});
```

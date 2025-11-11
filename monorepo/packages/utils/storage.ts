import Cookie from 'js-cookie';

const isStorageAvailable = (
  type: 'localStorage' | 'sessionStorage'
): boolean => {
  try {
    const storage = window[type];
    const x = '__storage_test__';
    storage.setItem(x, x);
    storage.removeItem(x);
    return true;
  } catch (_e) {
    return false;
  }
};

// In-memory fallback storage
const createInMemoryStorage = () => {
  const storage = new Map<string, string>();
  return {
    setItem: (key: string, value: string) => storage.set(key, value),
    getItem: (key: string) => storage.get(key) ?? null,
    removeItem: (key: string) => storage.delete(key),
    clear: () => storage.clear(),
  };
};

const inMemoryStorage = createInMemoryStorage();

export const storage = {
  localStorage: isStorageAvailable('localStorage')
    ? {
        set: <Item>(key: string, value: Item): void =>
          localStorage?.setItem(key, JSON.stringify(value)),
        get: <Item>(key: string): Item | undefined => {
          const item = localStorage?.getItem(key);
          return item ? (JSON.parse(item) as Item) : undefined;
        },
        remove: (key: string): void => localStorage?.removeItem(key),
        has: (key: string): boolean => localStorage.getItem(key) !== null,
        clear: () => localStorage.clear(),
      }
    : {
        set: <Item>(key: string, value: Item) =>
          inMemoryStorage.setItem(key, JSON.stringify(value)),
        get: <Item>(key: string): Item | undefined => {
          const item = inMemoryStorage.getItem(key);
          return item ? JSON.parse(item) : undefined;
        },
        remove: (key: string) => inMemoryStorage.removeItem(key),
        has: (key: string): boolean => inMemoryStorage.getItem(key) !== null,
        clear: () => inMemoryStorage.clear(),
      },
  cookieStorage: {
    set: <Item>(
      key: string,
      value: Item,
      options?: (typeof Cookie)['attributes']
    ) => Cookie?.set(key, JSON.stringify(value), options),
    get: <Item>(key: string): Item | undefined => {
      const item = Cookie?.get(key);
      if (!item) return undefined;
      try {
        return JSON.parse(item);
      } catch (_error) {
        // console.warn(`Failed to parse stored item for key "${key}":`, error);
        return item as Item; // Return the raw string if parsing fails
      }
    },
    remove: (key: string, options?: (typeof Cookie)['attributes']): void =>
      Cookie.remove(key, options),
    has: (key: string): boolean => Boolean(Cookie.get(key)),
  },
  sessionStorage: isStorageAvailable('sessionStorage')
    ? {
        set: <Item>(key: string, value: Item): void =>
          sessionStorage?.setItem(key, JSON.stringify(value)),
        get: <Item>(key: string): Item | undefined => {
          const item = sessionStorage?.getItem(key);
          return item ? JSON.parse(item) : undefined;
        },
        remove: (key: string): void => sessionStorage?.removeItem(key),
        has: (key: string): boolean => sessionStorage.getItem(key) !== null,
        clear: () => sessionStorage.clear(),
      }
    : {
        // Use the same in-memory fallback as localStorage
        set: <Item>(key: string, value: Item) =>
          inMemoryStorage.setItem(key, JSON.stringify(value)),
        get: <Item>(key: string): Item | undefined => {
          const item = inMemoryStorage.getItem(key);
          return item ? JSON.parse(item) : undefined;
        },
        remove: (key: string) => inMemoryStorage.removeItem(key),
        has: (key: string): boolean => inMemoryStorage.getItem(key) !== null,
        clear: () => inMemoryStorage.clear(),
      },
};

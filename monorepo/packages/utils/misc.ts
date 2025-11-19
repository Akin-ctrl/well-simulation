export const upperFirst = (value: string | undefined) =>
  typeof value !== 'string'
    ? ''
    : value.charAt(0).toUpperCase() + value.slice(1).toLowerCase();

export const formatCurrency = (value?: number | null, currency = '₦') => {
  return (
    currency +
    (value || 0)
      .toLocaleString('en', {
        currency: 'NGN',
        style: 'currency',
        compactDisplay: 'short',
      })
      .replace(/\.00/g, '')
      .replace('NGN', '')
      .trim()
  );
};

export const removeUndefined = (obj: { [key: string]: any }) => {
  for (const key of Object.keys(obj)) {
    if (obj[key] === undefined) {
      delete obj[key];
    }
  }
  return obj;
};

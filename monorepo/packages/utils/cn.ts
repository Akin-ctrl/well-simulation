import {clsx, type ClassValue} from 'clsx';
import {extendTailwindMerge} from 'tailwind-merge';

const customFontSizeRegex = /^(title|display|body|caption|subtitle)(-.*)?$/;

const customFontColorRegex = /^(fgColor|control|btn)(-.*)?$/;

function isCustomFontSize(value: string) {
  return customFontSizeRegex.test(value);
}
function isCustomFontColor(value: string) {
  return customFontColorRegex.test(value);
}

const customTwMerge = extendTailwindMerge({
  // ↓ Add values to existing class groups or define new ones
  extend: {
    classGroups: {
      'font-size': [{text: ['base', isCustomFontSize]}],
      'text-color': [{text: [isCustomFontColor]}],
    },
  },
});

export function cn(...inputs: ClassValue[]) {
  return customTwMerge(clsx(inputs));
}

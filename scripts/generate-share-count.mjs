import { countAllShares } from '../lib/share/storage.ts';

async function generateShareCount() {
  try {
    const totalCount = await countAllShares();
    console.log(`NEXT_PUBLIC_SHARE_COUNT=${totalCount}`);
  } catch (error) {
    console.error('Failed to generate share count:', error);
    console.log(`NEXT_PUBLIC_SHARE_COUNT=0`);
  }
}

generateShareCount();

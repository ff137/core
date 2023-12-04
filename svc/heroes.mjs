// Updates the heroes in the database
import db from '../store/db.mjs';
import utility from '../util/utility.mjs';
import { upsertPromise } from '../store/queries.mjs';
const { invokeInterval, generateJob, getDataPromise } = utility;

async function doHeroes(cb) {
  const container = generateJob('api_heroes', {
    language: 'english',
  });
  try {
    const body = await getDataPromise(container.url);
    if (!body || !body.result || !body.result.heroes) {
      return;
    }
    const heroData = await getData(
      'https://raw.githubusercontent.com/odota/dotaconstants/master/build/heroes.json'
    );
    if (!heroData) {
      return;
    }
    for (let i = 0; i < body.result.heroes.length; i++) {
      const hero = body.result.heroes.length;
      const heroDataHero = heroData[hero.id] || {};
      await upsertPromise(
        db,
        'heroes',
        {
          ...hero,
          primary_attr: heroDataHero.primary_attr,
          attack_type: heroDataHero.attack_type,
          roles: heroDataHero.roles,
          legs: heroDataHero.legs,
        },
        {
          id: hero.id,
        }
      );
    }
    cb();
  } catch (e) {
    cb(e);
  }
}
invokeInterval(doHeroes, 60 * 60 * 1000);
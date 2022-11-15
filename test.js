const db = require('../../models');
const { successResponse, errorResponse, paginatedList } = require('../../helpers');
const { Op } = require('sequelize');

/* TODO: Deprecate.
* This is a temporary patch to get list of Disciplines from the AI Output tables
* Ideally this should be done from the main list of Disciplines.
* This function should then be removed
*/

const fetchCommonFilterList = async (req, res, next) => {
  try {
    const query = `
    SELECT
      DISTINCT AD.DISCIPLINE_NAME AS VALUE,
      'Discipline' AS TYPE
    FROM
      AIPolarityData AD
    UNION
    SELECT
      DISTINCT AD.DIRECTORATE_NAME AS VALUE,
      'Directorate' AS TYPE
    FROM
      AIPolarityData AD
    UNION
    SELECT
      DISTINCT AD.LOCATION_NAME AS VALUE,
      'Location' AS TYPE
    FROM
      AIPolarityData AD` 
    const results = await db.sequelize.query(query, {nest: true});
    indexed = {};
    results.forEach(element => {
      if (indexed[element[`TYPE`]]) {
        indexed[element[`TYPE`]]
          .push(element['VALUE'])
      } else {
        indexed[element[`TYPE`]] 
          = [element['VALUE']]
      }
    });
    return successResponse(req, res, { data: indexed });

  } catch (err) {
    return next(err)
  }
}

const totalObservationOverview = async (req, res, next) => {
  try {
    const { discipline, directorate, location, card_type, view_by } = req.query;
    const view_by_filter = (view_by === 'location' ? 'ad.location_name' : 'ad.discipline_name');
    const discipline_filter = discipline ? `and ad.discipline_name ='${discipline}'`: '';
    const directorate_filter = directorate ? `and ad.directorate_name ='${directorate}'`: '';
    const location_filter = location;
    const card_type_filter = card_type;
    const query = `SELECT 
    ${view_by === 'location' ? 'ad.location_name' : 'ad.discipline_name'}, 
    COUNT(ad.polarity)as count, MONTH(date) as month 
    FROM AIPolarityData ad 
    WHERE 1=1
    ${discipline ? `and ad.discipline_name ='${discipline}'`: ''}
    ${directorate ? `and ad.directorate_name ='${directorate}'`: ''}
    ${location ? `and ad.location_name ='${location}'`: ''}
    ${card_type ? `and ad.Card_type = '${card_type}'`: ''}
    GROUP BY ${view_by === 'location' ? 'ad.location_name' : 'ad.discipline_name'}, MONTH(ad.date) 
    ORDER BY ${view_by === 'location' ? 'ad.location_name' : 'ad.discipline_name'}, month`;
    const results = await db.sequelize.query(query, {nest: true});
    indexed = {};
    data = [];
    results.forEach(element => {

      if (indexed[element[`${view_by === 'location' ? 'location_name' : 'discipline_name'}`]]) {
        indexed[element[`${view_by === 'location' ? 'location_name' : 'discipline_name'}`]]
          .push({ x: `${element['month']}`, y: element['count'] })
      } else {
        indexed[element[`${view_by === 'location' ? 'location_name' : 'discipline_name'}`]] 
          = [{ x: `${element['month']}`, y: element['count'] }]
      }
    });
    Object.keys(indexed).forEach((element) => {
      data.push({
        name: element,
        data: indexed[element]
      });
    });
    filters = [
      {key: "card_type", values: [
        { label: 'Safe', value:'Safe' },
        { label: 'Unsafe', value:'Unsafe' },
        { label: 'All', value:'' }
      ]},
      {key: "view_by", values: [
        { label: 'Location', value:'location' },
        { label: 'Discipline', value:'discipline' }
      ]}
    ]
    return successResponse(req, res, { data, filters });
  } catch (err) {
    return next(err);
  }
}

const percentTotals = async (req, res, next) => {
  try {
    const { discipline, directorate, location, card_type, view_by } = req.query;
    const query = `SELECT 
    ad.discipline_name, ad.location_name, 
    ${ view_by === 'action' 
      ? 'ROUND(SUM(CAST(  ad.action_performed as float)) / COUNT(ad.action_performed)*100, 2)' 
      : 'COUNT(ad.polarity)' } AS computed_value 
    FROM AIPolarityData ad 
    WHERE 1=1
    ${discipline ? `and ad.discipline_name ='${discipline}'`: ''}
    ${directorate ? `and ad.directorate_name ='${directorate}'`: ''}
    ${location ? `and ad.location_name ='${location}'`: ''}
    ${card_type ? `and ad.Card_type = '${card_type}'`: ''}
    GROUP BY ad.discipline_name,ad.location_name ORDER BY ad.discipline_name , ad.location_name`;
    const results = await db.sequelize.query(query, {nest: true});
    indexed = {};
    data = [];
    results.forEach(element => {

      if (indexed[element[`location_name`]]) {
        indexed[element[`location_name`]]
          .push({ x: element['discipline_name'], y: element['computed_value'] })
      } else {
        indexed[element[`location_name`]] 
          = [{ x: element['discipline_name'], y: element['computed_value'] }]
      }
    });
    Object.keys(indexed).forEach((element) => {
      data.push({
        name: element,
        data: indexed[element]
      });
    });
    filters = [
      {key: "card_type", values: [
        { label: 'Safe', value:'Safe' },
        { label: 'Unsafe', value:'Unsafe' },
        { label: 'All', value:'' }
      ]},
      {key: "view_by", values: [
        { label: 'Action', value:'action' },
        { label: 'Total', value:'total' }
      ]}
    ]
    return successResponse(req, res, { data, filters });
  } catch (err) {
    return next(err);
  }
}

const sentimentByLocation = async (req, res, next) => {
  try {
    const { discipline, directorate, location, card_type } = req.query;
    const query = `SELECT ad.discipline_name, ad.location_name, ROUND(AVG(ad.polarity), 2) as computed_value 
    FROM AIPolarityData ad 
    WHERE 1=1
    ${discipline ? `and ad.discipline_name ='${discipline}'`: ''}
    ${directorate ? `and ad.directorate_name ='${directorate}'`: ''}
    ${location ? `and ad.location_name ='${location}'`: ''}
    ${card_type ? `and ad.Card_type = '${card_type}'`: ''}
    GROUP BY ad.discipline_name,ad.location_name ORDER BY ad.discipline_name , ad.location_name`;
    const results = await db.sequelize.query(query, {nest: true});
    indexed = {};
    data = [];
    results.forEach(element => {

      if (indexed[element[`location_name`]]) {
        indexed[element[`location_name`]]
          .push({ x: element['discipline_name'], y: element['computed_value'] })
      } else {
        indexed[element[`location_name`]] 
          = [{ x: element['discipline_name'], y: element['computed_value'] }]
      }
    });
    Object.keys(indexed).forEach((element) => {
      data.push({
        name: element,
        data: indexed[element]
      });
    });
    filters = [
      {key: "card_type", values: [
        { label: 'Safe', value:'Safe' },
        { label: 'Unsafe', value:'Unsafe' },
        { label: 'All', value:'' }
      ]}
    ]
    return successResponse(req, res, { data, filters });
  } catch (err) {
    return next(err);
  }
}

const topAndBottomBehaviorBySentiment = async (req, res, next) => {
  try {
    const { discipline, directorate, location, card_type, view_by } = req.query;
    const query = `SELECT * 
    FROM (
      SELECT TOP(5) ad.behavior_name, ROUND(AVG(ad.polarity), 2) AS sentiment 
      FROM AIPolarityData ad 
      WHERE 1=1
      ${directorate ? `AND ad.directorate_name ='${directorate}'`: ''}
      ${discipline ? `AND ad.discipline_name ='${discipline}'`: ''} 
      ${location ? `AND ad.location_name ='${location}'`: ''} 
      ${card_type ? `AND ad.Card_type = '${card_type}'`: ''}
      GROUP BY ad.behavior_name 
      ORDER BY sentiment ASC
    ) a
    UNION
    SELECT * FROM (
      SELECT TOP(5) ad.behavior_name, ROUND(AVG(ad.polarity), 2) AS sentiment 
      FROM AIPolarityData ad 
      WHERE 1=1
      ${directorate ? `AND ad.directorate_name ='${directorate}'`: ''}
      ${discipline ? `AND ad.discipline_name ='${discipline}'`: ''} 
      ${location ? `AND ad.location_name ='${location}'`: ''} 
      ${card_type ? `AND ad.Card_type = '${card_type}'`: ''} 
      GROUP BY ad.behavior_name 
      ORDER BY sentiment DESC
    ) b
    ORDER BY sentiment`;
    const results = await db.sequelize.query(query, {nest: true});
    categories = [];
    data = [{
      name: 'Top and Bottom Behaviors by Sentiment',
      data: []
    }];
    results.forEach(element => {
      categories.push(element[`behavior_name`]);
      data[0].data.push(element['sentiment']);
    });
    filters = [
      {key: "card_type", values: [
        { label: 'Safe', value:'Safe' },
        { label: 'Unsafe', value:'Unsafe' },
        { label: 'All', value:'' }
      ]}
    ]
    return successResponse(req, res, { categories, data, filters });
  } catch (err) {
    return next(err);
  }
}


const wordCloud = async (req, res, next) => {
  try {
    const { discipline, directorate, location, card_type, view_by } = req.query;
    const query = `SELECT TOP(30) ac.Word, sum(ac.Count) as Count FROM AIWordCloud ac WHERE 1=1
    ${directorate ? `AND ac.directorate_name ='${directorate}'`: ''}
    ${discipline ? `AND ac.discipline_name ='${discipline}'`: ''} 
    ${location ? `AND ac.location_name ='${location}'`: ''} 
    ${card_type ? `AND ac.Card_type = '${card_type}'`: ''}
    GROUP BY ac.Word 
    ORDER BY Count DESC`;
    const results = await db.sequelize.query(query, {nest: true});
    data = [];
    results.forEach(element => {
      data.push({
        text: element['Word'],
        value: element['Count']
      })
    });
    filters = [
      {key: "card_type", values: [
        { label: 'Safe', value:'Safe' },
        { label: 'Unsafe', value:'Unsafe' },
        { label: 'All', value:'' }
      ]}
    ]
    return successResponse(req, res, { data, filters });
  } catch (err) {
    return next(err);
  }
}

const nGramsBarChart = async (req, res, next) => {
  try {
    const { discipline, directorate, location, card_type, view_by } = req.query;
    let query = '';
    if (view_by === 'trigrams') {
      query = `SELECT TOP(50) at2.trigrams as ngram, sum(at2.Count) as count FROM AiTrigrams at2 WHERE 1=1
      ${directorate ? `AND at2.directorate_name ='${directorate}'`: ''}
      ${discipline ? `AND at2.discipline_name ='${discipline}'`: ''} 
      ${location ? `AND at2.location_name ='${location}'`: ''} 
      ${card_type ? `AND at2.Card_type = '${card_type}'`: ''}
      GROUP BY at2.trigrams  
      ORDER BY Count DESC`
    } else {
      query = `SELECT TOP(50) ab.bigram as ngram,sum(ab.Count) as count FROM AiBigrams ab WHERE 1=1
      ${directorate ? `AND ab.directorate_name ='${directorate}'`: ''}
      ${discipline ? `AND ab.discipline_name ='${discipline}'`: ''} 
      ${location ? `AND ab.location_name ='${location}'`: ''} 
      ${card_type ? `AND ab.Card_type = '${card_type}'`: ''}
      GROUP BY ab.bigram 
      ORDER BY Count DESC`;
    }
    const results = await db.sequelize.query(query, {nest: true});
    categories = [];
    data = [{
      name: view_by === 'trigrams'? 'Trigrams' : 'Bigrams',
      data: []
    }];
    results.forEach(element => {
      categories.push(element[`ngram`]);
      data[0].data.push(element['count']);
    });
    filters = [
      {key: "card_type", values: [
        { label: 'Safe', value:'Safe' },
        { label: 'Unsafe', value:'Unsafe' },
        { label: 'All', value:'' }
      ]},
      {key: "view_by", values: [
        { label: 'Bigrams', value:'bigrams' },
        { label: 'Trigrams', value:'trigrams' }
      ]}
    ]
    return successResponse(req, res, { categories, data, filters });
  } catch (err) {
    return next(err);
  }
}

const qualityIndex = async (req, res, next) => {
  try {
    const { discipline, directorate, location, card_type, view_by } = req.query;
    let query = '';
    if (view_by === 'location') {
      query = `SELECT  ad.location_name, 
      ROUND(AVG(ad.length_of_text), 2) as Feedback , 
      ROUND(AVG(ad.comment_len), 2) as Comment, 
      ROUND(AVG(ad.rootcause_len), 2) as Rootcause 
      FROM AIPolarityData ad 
      WHERE 1=1
      ${directorate ? `AND ad.directorate_name ='${directorate}'`: ''}
      ${discipline ? `AND ad.discipline_name ='${discipline}'`: ''} 
      ${location ? `AND ad.location_name ='${location}'`: ''} 
      ${card_type ? `AND ad.Card_type = '${card_type}'`: ''}
      GROUP BY ad.location_name`
    } else {
      query = `SELECT ad.discipline_name, 
      ROUND(AVG(ad.length_of_text), 2) as Feedback, 
      ROUND(AVG(ad.comment_len), 2) as Comment, 
      ROUND(AVG(ad.rootcause_len), 2) as Rootcause 
      FROM AIPolarityData ad 
      WHERE 1=1
      ${directorate ? `AND ad.directorate_name ='${directorate}'`: ''}
      ${discipline ? `AND ad.discipline_name ='${discipline}'`: ''} 
      ${location ? `AND ad.location_name ='${location}'`: ''} 
      ${card_type ? `AND ad.Card_type = '${card_type}'`: ''}
      GROUP BY ad.discipline_name`;
    }
    const results = await db.sequelize.query(query, {nest: true});
    categories = [];
    data = [{
      name: 'Feedback',
      data: []
    },{
      name: 'Comment',
      data: []
    },{
      name: 'Rootcause',
      data: []
    }];
    results.forEach(element => {
      categories.push(element[`location_name`]);
      data[0].data.push(element['Feedback']);
      data[1].data.push(element['Comment']);
      data[2].data.push(element['Rootcause']);
    });
    filters = [
      {key: "card_type", values: [
        { label: 'Safe', value:'Safe' },
        { label: 'Unsafe', value:'Unsafe' },
        { label: 'All', value:'' }
      ]},
      {key: "view_by", values: [
        { label: 'Location', value:'location' },
        { label: 'Discipline', value:'discipline' }
      ]}
    ]
    return successResponse(req, res, { categories, data, filters });
  } catch (err) {
    return next(err);
  }
}

const topAndBottomFeedbackBySentiment = async (req, res, next) => {
  try {
    const { discipline, directorate, location, card_type, view_by } = req.query;
    const query = `SELECT * 
    FROM (
      SELECT TOP(5) ad.feedback, ROUND(AVG(ad.polarity), 2) AS sentiment 
      FROM AIPolarityData ad 
      WHERE 1=1
      ${directorate ? `AND ad.directorate_name ='${directorate}'`: ''}
      ${discipline ? `AND ad.discipline_name ='${discipline}'`: ''} 
      ${location ? `AND ad.location_name ='${location}'`: ''} 
      ${card_type ? `AND ad.Card_type = '${card_type}'`: ''}
      GROUP BY ad.feedback 
      ORDER BY sentiment ASC) a
    UNION
    SELECT * 
    FROM (
      SELECT TOP(5) ad.feedback, ROUND(AVG(ad.polarity), 2) AS sentiment 
      FROM AIPolarityData ad 
      WHERE 1=1
      ${directorate ? `AND ad.directorate_name ='${directorate}'`: ''}
      ${discipline ? `AND ad.discipline_name ='${discipline}'`: ''} 
      ${location ? `AND ad.location_name ='${location}'`: ''} 
      ${card_type ? `AND ad.Card_type = '${card_type}'`: ''}
      GROUP BY ad.feedback 
      ORDER BY sentiment DESC) b
    ORDER BY sentiment`;
    const results = await db.sequelize.query(query, {nest: true});
    categories = [];
    data = [{
      name: 'Top and Bottom Feedback by Sentiment',
      data: []
    }];
    results.forEach(element => {
      categories.push(element[`feedback`]);
      data[0].data.push(element['sentiment']);
    });
    filters = [
      {key: "card_type", values: [
        { label: 'Safe', value:'Safe' },
        { label: 'Unsafe', value:'Unsafe' },
        { label: 'All', value:'' }
      ]}
    ]
    return successResponse(req, res, { categories, data, filters });
  } catch (err) {
    return next(err);
  }
}

const riskHeatMap = async (req, res, next) => {
  try {
    const { discipline, directorate, location, card_type, view_by } = req.query;
    const query = `SELECT ad.discipline_name , ad.location_name, ROUND(AVG(ad.risk), 2) as  Risk 
    FROM AIPolarityData ad 
    WHERE 1=1
    ${discipline ? `and ad.discipline_name ='${discipline}'`: ''}
    ${directorate ? `and ad.directorate_name ='${directorate}'`: ''}
    ${location ? `and ad.location_name ='${location}'`: ''}
    ${card_type ? `and ad.Card_type = '${card_type}'`: ''}
    GROUP BY ad.discipline_name,ad.location_name 
    ORDER BY ad.discipline_name , ad.location_name`;
    const results = await db.sequelize.query(query, {nest: true});
    indexed = {};
    data = [];
    results.forEach(element => {

      if (indexed[element['discipline_name']]) {
        indexed[element['discipline_name']]
          .push({ x: `${element['location_name']}`, y: element['Risk'] })
      } else {
        indexed[element['discipline_name']]
          = [{ x: `${element['location_name']}`, y: element['Risk'] }]
      }
    });
    Object.keys(indexed).forEach((element) => {
      data.push({
        name: element,
        data: indexed[element]
      });
    });
    filters = [
      {key: "card_type", values: [
        { label: 'Safe', value:'Safe' },
        { label: 'Unsafe', value:'Unsafe' },
        { label: 'All', value:'' }
      ]}
    ]
    return successResponse(req, res, { data, filters });
  } catch (err) {
    return next(err);
  }
}

const riskBarGraph = async (req, res, next) => {
  try {
    const { discipline, directorate, location, card_type, view_by } = req.query;
    const query = `Select ROUND(AVG( ad.action_performed_pct ), 2) as action_completion_rate, 
    ROUND(AVG( ad.card_type_val_pct ), 2) as safe_score, ROUND(AVG( ad.polarity_pct), 2) as sentiment 
    FROM AIPolarityData ad
    WHERE 1=1
    ${discipline ? `and ad.discipline_name ='${discipline}'`: ''}
    ${directorate ? `and ad.directorate_name ='${directorate}'`: ''}
    ${location ? `and ad.location_name ='${location}'`: ''}
    ${card_type ? `and ad.Card_type = '${card_type}'`: ''}`;
    const results = await db.sequelize.query(query, {nest: true});
    categories = ['Action Completion Rate', 'Safe Score', 'Sentiment'];
    data = [{
      name: 'Action Completion Rate',
      data: []
    },{
      name: 'Safe Score',
      data: []
    },{
      name: 'Sentiment',
      data: []
    }];
    results.forEach(element => {
      data[0].data.push(element['action_completion_rate']);
      data[1].data.push(element['safe_score']);
      data[2].data.push(element['sentiment']);
    });
    filters = [
      {key: "card_type", values: [
        { label: 'Safe', value:'Safe' },
        { label: 'Unsafe', value:'Unsafe' },
        { label: 'All', value:'' }
      ]}
    ]
    return successResponse(req, res, { categories, data, filters });
  } catch (err) {
    return next(err);
  }
}

const topBehaviors = async (req, res, next) => {
  try {
    const { discipline, directorate, location, card_type, view_by } = req.query;
    const query = `select TOP(20) ad.behavior_name, COUNT(ad.polarity) as sentiment 
    FROM AIPolarityData ad 
    WHERE 1=1
    ${discipline ? `and ad.discipline_name ='${discipline}'`: ''}
    ${directorate ? `and ad.directorate_name ='${directorate}'`: ''}
    ${location ? `and ad.location_name ='${location}'`: ''}
    ${card_type ? `and ad.Card_type = '${card_type}'`: ''} 
    group by ad.behavior_name  order by sentiment  Desc`;
    const results = await db.sequelize.query(query, {nest: true});
    categories = [];
    data = [{
      name: 'Top Behaviors',
      data: []
    }];
    results.forEach(element => {
      categories.push(element['behavior_name'])
      data[0].data.push(element['sentiment']);
    });
    filters = [
      {key: "card_type", values: [
        { label: 'Safe', value:'Safe' },
        { label: 'Unsafe', value:'Unsafe' },
        { label: 'All', value:'' }
      ]}
    ]
    return successResponse(req, res, { categories, data, filters });
  } catch (err) {
    return next(err);
  }
}

module.exports = {
  fetchCommonFilterList,
  totalObservationOverview,
  percentTotals,
  sentimentByLocation,
  topAndBottomBehaviorBySentiment,
  wordCloud,
  nGramsBarChart,
  qualityIndex,
  topAndBottomFeedbackBySentiment,
  riskHeatMap,
  riskBarGraph,
  topBehaviors
}
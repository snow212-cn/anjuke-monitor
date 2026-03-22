/**
 * 可选：通知扩展点示例（不参与默认运行）
 *
 * 用法：
 * - CLI: node anjuke_monitor.js --notify ./config/notify.example.js
 * - ENV: AJ_NOTIFY_MODULE=./config/notify.example.js
 *
 * 说明：
 * - 本项目在发生变更时，会在 context.text 提供“人类可读”的通知正文（推荐直接发这个）
 * - diff 仍然提供结构化数据，便于你二次加工
 */

'use strict';

/**
 * @param {{added:any[], removed:any[], updated:any[]}} diff
 * @param {{target:any, fetchedAt?:string, listHash?:string, summary?:any, text?:string}} context
 */
module.exports = async function notify(diff, context) {
  // 这里写你自己的通知逻辑
  // 示例（青龙常见）：
  // const { sendNotify } = require('./sendNotify');
  const title = `【房源变更】 ${context.target.name}`;
  const content = context.text || JSON.stringify(diff, null, 2);
  // await sendNotify(title, content);
  console.log(QLAPI.systemNotify({"title": title, "content": content}))

  // 默认示例：打印“人类可读”正文（如果没有则回退到 JSON）
  console.log('[notify.example] title=', `【房源变更】 ${context.target?.name || ''}`);
  console.log(context?.text || JSON.stringify(diff, null, 2));
};

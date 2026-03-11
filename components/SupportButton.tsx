"use client";

import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";

const donationAcknowledgements: Array<{
  date: string;
  amount: string;
  message: string;
}> = [
  { date: "2026-03-11", amount: "1", message: "" },
  { date: "2026-03-11", amount: "50", message: "旻，屹立于大地之上！" },
  { date: "2026-03-11", amount: "20", message: "喝个奶茶再接再厉🤣" },
  { date: "2026-03-11", amount: "10", message: "" },
  { date: "2026-03-11", amount: "5.2", message: "老大加油！想问问网站搜索动画能加一个炫斗战轮吗？（开发者注：已在bangumi添加条目）​" },
  { date: "2026-03-11", amount: "100", message: "坏了我微信基本没存啥钱，下次加个支付宝的吧.jpg ​​​" },
  { date: "2026-03-11", amount: "50", message: "请阿旻老师吃kfc" },
  { date: "2026-03-11", amount: "20", message: "可惜保存图片失效了" },
  { date: "2026-03-11", amount: "52", message: "Love explosions！" },
  { date: "2026-03-11", amount: "100", message: "希望旻妈妈增加拖拽功能！" },
  { date: "2026-03-11", amount: "1", message: "加油" },
  { date: "2026-03-10", amount: "50", message: "虽然今天不是星期四但是看起来已经足够疯狂了" },
  { date: "2026-03-10", amount: "79.2", message: "" },
  { date: "2026-03-10", amount: "100", message: "加油啊旻妈妈……" },
];

const collectedCount = process.env.NEXT_PUBLIC_SHARE_COUNT
  ? parseInt(process.env.NEXT_PUBLIC_SHARE_COUNT, 10)
  : null;

export function SupportButton() {
  const wechatPayQrUrl = process.env.NEXT_PUBLIC_WECHAT_PAY_QR_URL?.trim();
  const fallbackWechatPayQrUrl = "/wechatpay.png";
  const [wechatPayQrSrc, setWechatPayQrSrc] = useState(wechatPayQrUrl ?? fallbackWechatPayQrUrl);

  return (
    <Dialog>
      <DialogTrigger asChild>
        <button
          type="button"
          className="bg-transparent p-0 text-red-600 text-sm transition-colors hover:text-slate-800 hover:underline"
        >
          支援开发者
        </button>
      </DialogTrigger>
      <DialogContent className="w-[92vw] max-w-xl max-h-[85vh] overflow-y-auto rounded-2xl p-5">
        <DialogHeader className="text-left">
          <DialogTitle>感谢支持</DialogTitle>
          <DialogDescription className="space-y-1.5 text-slate-600">
            <span className="block">
              本项目上线至今已经建构了{" "}
              <span className="font-semibold text-sky-600">
                {collectedCount === null ? "..." : collectedCount.toLocaleString("zh-CN")}
              </span>{" "}
              份大家的构成！可喜可贺（啪叽啪叽）
            </span>
            <span className="block">
              但与此同时，意料之外的流行也让服务器开始不堪重负……<span style={{ textDecoration: "line-through" }}>（我草怎么真的炸了）</span>
            </span>
            <span className="block">
              虽然在努力想办法自己解决，如果有谁愿意帮忙就太好了呢。
            </span>
            <span className="block">
              也非常欢迎通过
              <a
                href="https://github.com/SomiaWhiteRing/my9"
                target="_blank"
                rel="noreferrer"
                className="font-semibold text-sky-600 underline decoration-sky-300 underline-offset-2 hover:text-sky-700"
              >
                在 GitHub 点 Star
              </a>
              提供精神支持！
            </span>
          </DialogDescription>
        </DialogHeader>
        {wechatPayQrUrl ? (
          <div className="mt-3 flex justify-center">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src={wechatPayQrSrc}
              alt="微信赞赏码"
              className="h-60 w-60 rounded-lg border border-slate-200 object-contain"
              onError={() => {
                setWechatPayQrSrc((current) =>
                  current === fallbackWechatPayQrUrl ? current : fallbackWechatPayQrUrl
                );
              }}
            />
          </div>
        ) : (
          <p className="mt-3 text-left text-sm text-slate-500">
            暂未配置微信赞赏码。请在 <code>.env.local</code> 设置
            <code> NEXT_PUBLIC_WECHAT_PAY_QR_URL</code>。
          </p>
        )}
        <section className="mt-5 border-t border-slate-200 pt-4">
          <h3 className="text-sm font-semibold text-slate-700">鸣谢名单</h3>
          <p className="mt-1 text-sm text-slate-500">
            非常非常非常感谢以下各位的支持让站点能够运营下来……（排序从新到旧）
          </p>
          <p className="mt-1 text-sm text-slate-500">
            各位的支持会成为站点存续的基石和我更新维护的动力！
          </p>
          <div className="mt-3 overflow-hidden rounded-lg border border-slate-200">
            <table className="w-full table-fixed text-left text-xs text-slate-600">
              <thead className="bg-slate-50 text-[11px] font-semibold text-slate-500">
                <tr>
                  <th className="w-28 px-3 py-2">打赏日期</th>
                  <th className="w-24 px-3 py-2">打赏金额</th>
                  <th className="px-3 py-2">附言</th>
                </tr>
              </thead>
              <tbody>
                {donationAcknowledgements.map((item, index) => (
                  <tr
                    key={`${item.date}-${item.amount}-${index}`}
                    className="border-t border-slate-100"
                  >
                    <td className="px-3 py-2 align-top whitespace-nowrap font-medium">
                      {item.date}
                    </td>
                    <td className="px-3 py-2 align-top whitespace-nowrap font-semibold">
                      {item.amount}
                    </td>
                    <td className="px-3 py-2 align-top break-words">
                      {item.message}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </DialogContent>
    </Dialog>
  );
}

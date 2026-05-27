// 给 CSS / 静态资源副作用 import 一个空类型，免得新 TS 在 strict 模式下报
// "Cannot find module or type declarations for side-effect import of '*.css'"
declare module '*.css';
declare module '*.svg';
declare module '*.png';

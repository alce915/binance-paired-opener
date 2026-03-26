# Binance Paired Opener

鍩轰簬 Binance USD-M Futures 鐨勫弻鍚戦厤瀵瑰紑鍗曟湇鍔★紝褰撳墠鍖呭惈涓ゅ鐙珛鍏ュ彛锛?
- 寮€鍗曟帶鍒跺彴涓庡紑鍗?API锛岄粯璁ょ鍙?`8000`
- 璐︽埛鐩戞帶鎺у埗鍙颁笌鐩戞帶 API锛岄粯璁ょ鍙?`8010`

## 瀹夎

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
Copy-Item .env.example .env
```

## 鍚姩寮€鍗曟湇鍔?
鍛戒护琛岋細

```powershell
paired-opener-api
```

鑴氭湰锛?
```powershell
scripts\restart_service.bat
```

璁块棶鍦板潃锛?
- 寮€鍗曟帶鍒跺彴: `http://127.0.0.1:8000/`
- OpenAPI: `http://127.0.0.1:8000/docs`

## 鍚姩鐙珛鐩戞帶鏈嶅姟

鍛戒护琛岋細

```powershell
paired-opener-monitor-api
```

鑴氭湰锛?
```powershell
scripts\restart_monitor_service.bat
```

璁块棶鍦板潃锛?
- 鐩戞帶鎺у埗鍙? `http://127.0.0.1:8010/`
- 鍋ュ悍妫€鏌? `http://127.0.0.1:8010/healthz`
- 鐩戞帶蹇収鎺ュ彛: `http://127.0.0.1:8010/api/accounts`
- 鐩戞帶 SSE: `http://127.0.0.1:8010/stream/accounts`

## 璐︽埛閰嶇疆

鐩戞帶鏈嶅姟鏀寔閫氳繃閰嶇疆鏂囦欢鏂板璐︽埛锛?
- 绀轰緥鏂囦欢: `config\binance_accounts.example.json`
- 瀹為檯鏂囦欢: `config\binance_accounts.json`

## CLI 绀轰緥

```powershell
paired-opener create BTCUSDT long 50 3 0.001
paired-opener list
```

## 甯歌鎺掓煡

- `http://127.0.0.1:8010/` 鏃犳硶璁块棶锛氶€氬父鏄洃鎺ф湇鍔℃湭鍚姩锛屽厛鎵ц `scripts\restart_monitor_service.bat`
- `8010` 绔彛琚崰鐢細鏂拌剼鏈細鍏堝皾璇曢噴鏀剧洃鍚繘绋嬶紝鍐嶅惎鍔ㄧ洃鎺ф湇鍔?- 鍋ュ悍妫€鏌ュけ璐ワ細鍏堣闂?`http://127.0.0.1:8010/healthz`锛屽啀鏌ョ湅 `monitor.runtime.log`
- 鏃ф湇鍔′笉鍙楀奖鍝嶏細`scripts\restart_service.bat` 鍙礋璐?`8000`锛屼笉浼氬惎鍔ㄦ垨鍋滄 `8010`

## 娉ㄦ剰

- 鏈郴缁熶笉浼氫繚璇佲€滀笉鐖嗕粨鈥濓紝鍙礋璐ｆ寜绛栫暐鎵ц寮€鍗曘€佹暟閲忓榻愪笌寮傚父瀹¤
- 褰撳墠鐗堟湰鑱氱劍寮€鍗曢摼璺笌璐︽埛鐩戞帶锛屼笉鍖呭惈瀹屾暣骞充粨銆佸噺浠撲笌椋庢帶鎵樼鑳藉姏
- 鎺ュ叆涓荤綉鍓嶏紝寤鸿鍏堝湪娴嬭瘯鐜楠岃瘉璐︽埛妯″紡銆佸弬鏁般€佽疆璇㈤鐜囦笌鏉冮檺閰嶇疆

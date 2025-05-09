// Parameters
var target_stock = [];
var ListingDate_stock = [];
var DelistingDate_stock = [];
var code_modified = [];
var target_stock_code =[];

var target_sector = [];
var te = []; // total equity, 천원, PBR 계산시 사용
var cap = []; // 시가총액, 백만원, PBR 계산시 사용
var rv = []; // 매출, 천원
var cfo = []; // 영업현금흐름, 천원
var cogs = []; // 매출원가, 천원, gp = rv - cogs로 계산한다.
var t_a = []; // total asset, 천원
var np = []; // net profit, 천원
var tl = []; // Total Liability, 천원
var oi = []; // operating income, 천원
var ie = []; //interest expense, 천원
var ev = []; // EV,  천원
var ebitda = []; // EBITDA, 천원
var ebit = []; // 천원
var nl= []; // net liability, 천원
var cfo = []; // operating cash flow(cash from operation), 천원
var capex = []; // capex, 천원
var depre = []; // depreciation(감가상각비(판관비)), 천원
var rnd = []; // 연구개발비, 천원
var inven = []; // 재고자산, 천원
var ca = []; // current asset(유동자산), 천원
var cl = []; // current liability(유동부채), 천원
var r_e = []; // retained earn(이익잉여금), 천원

//var GP = [];

var code_listed = 'A005930'; // 삼성전자. 2000년부터 계속 상장된 회사
var stock_listed;
var load_failure_list = [];  // 상폐일, 상장일이 기준일 조건을 충족하는데 종목 조회 실패한 데이터들의 인덱스
var DelistingDate_Error_list = [];	// 상폐일이 시뮬레이션 시작일보다 빠른 데이터들의 인덱스

var month_quarter_4 = 3; //4월. 전년도 4분기 데이터 업데이트
var month_quarter_1 = 5; //6월. 이번년도 1분기 데이터 업데이트
var month_quarter_2 = 8; //9월. 이번년도 2분기 데이터 업데이트
var month_quarter_3 = 11; //12월. 이번년도 3분기 데이터 업데이트
var last_quarter_4_rebal_year = 99; // 초기값을 이렇게 잡아야 2000년 1월 4일부터 돌릴때 2000년에 처음 동작한다.
var last_quarter_1_rebal_year = 99;
var last_quarter_2_rebal_year = 99;
var last_quarter_3_rebal_year = 99;

var list_index = 0;
var NumOfStocks = 0;
var len_data = code.length;
var last_data_day;
var first_data_day = new Date('2000-04-03');

function removeA(str) {
    return str.replace(/^A/, '');
}
function roundToDecimal(value, decimalPlaces) {
    var factor = Math.pow(10, decimalPlaces);
    return Math.round(value * factor) / factor;
}
function getData(i){
    te[i] = target_stock[i].getFundamentalTotalEquity(); // total equity, 천원
    cap[i] =  target_stock[i].getMarketCapital(); // 시가총액, 백만원
    rv[i] = target_stock[i].getFundamentalRevenue(); // 매출액 [천원]
    cfo[i] = target_stock[i].getFundamentalOperatingCashFlow(); // 영업현금흐름 [천원]
    cogs[i] = target_stock[i].getFundamentalSalesCost(); // 매출원가, 천원, gp = rv - cogs로 계산한다.
    t_a[i] = target_stock[i].getFundamentalTotalAsset(); // total asset, 천원
    np[i] = target_stock[i].getFundamentalNetProfit(); //net profit, 천원
    tl[i] = target_stock[i].getFundamentalTotalLiability(); // Total Liability, 천원
    oi[i] = target_stock[i].getFundamentalOperatingIncome(); // operating income, 천원
    ie[i] = target_stock[i].getFundamentalInterestExpense(); //interest expense, 천원
    ev[i] = target_stock[i].getFundamentalEV(); //EV,  천원
    ebitda[i] = target_stock[i].getFundamentalEBITDA(); // EBITDA, 천원
    ebit[i] = target_stock[i].getFundamentalEBIT(); // EBIT, 천원
    nl[i] = target_stock[i].getFundamentalNetLiability(); // 순부채, 천원
    capex[i] = target_stock[i].getFundamentalCAPEX(); //capex, 천원
    depre[i] = target_stock[i].getFundamentalDepreciationCost(); // depreciation(감가상각비(판관비)), 천원
    rnd[i] = target_stock[i].getFundamentalRnDExpense(); //연구개발비, 천원
    inven[i] = target_stock[i].getFundamentalInventoryAsset(); //  재고자산, 천원
    ca[i] = target_stock[i].getFundamentalCurrentAsset(); // current asset(유동자산), 천원
    cl[i] = target_stock[i].getFundamentalCurrentLiability(); //  current liability(유동부채), 천원
    r_e[i] = target_stock[i].getFundamentalRetainedEarn(); // retained earn(이익잉여금), 천원
    logger.info(removeA(target_stock_code[i]) + ', sector: ' + target_stock[i].sector + ', te: ' + te[i] + ', cap: ' + cap[i] + ', rv: ' + rv[i] + ', cfo: ' + cfo[i]
               + ', cogs: ' + cogs[i] + ', t_a: ' + t_a[i] + ', np: ' + np[i] + ', tl: ' + tl[i]
               + ', oi: ' +  oi[i] + ', ie: ' + ie[i] + ', ev: ' + ev[i] + ', ebitda: ' + ebitda[i]
                + ', ebit: ' +  ebit[i] + ', nl: ' + nl[i] + ', capex: ' + capex[i] + ', depre: ' + depre[i]
               + ', rnd: ' + rnd[i] + ', inven: ' + inven[i] + ', ca: ' + ca[i] + ', cl: ' + cl[i] + ', r_e: ' + r_e[i]);
}


function initialize() {
    for (var i = 0; i < len_data; i++) {
    	code_modified[i] = code[i].substring(0, code[i].length - 1) + '0'; // 코드명 마지막 글자 '0'으로 변경
	}
    for (var i = 0; i < len_data; i++) {
    	ListingDate[i].setHours(0, 0, 0, 0);
	}
    for (var i = 0; i < len_data; i++) {
    	DelistingDate[i].setHours(0, 0, 0, 0);
	}
    StartDate.setHours(0, 0, 0, 0);
    FinalDate.setHours(0, 0, 0, 0);
    stock_listed = IQStock.getStock(code_listed);

    while ((ListingDate[list_index] <= StartDate) && (list_index < len_data)){ // 더 최근 날짜가 더 크다
        if (DelistingDate[list_index] >= StartDate) {//DelistingDate 보다 기준일이 더 과거인지 확인
			var _stock = IQStock.getStock(code_modified[list_index]);
            if (_stock != null)
            {
                target_stock[NumOfStocks] = _stock;
                target_stock_code[NumOfStocks] = code[list_index]; // target stock의 original 코드명 맵핑
                target_sector[NumOfStocks] = _stock.sector; // targe stock의 sector
                ListingDate_stock[NumOfStocks] = ListingDate[list_index]; // target stock의 상장일 맵핑
                DelistingDate_stock[NumOfStocks] = DelistingDate[list_index]; // target stock의 상폐일 맵핑
                NumOfStocks++;
                logger.info('_stock: ' + _stock.name + ', ' + NumOfStocks );
                //_stock.loadPrevData(1,0,0);
            }
            else
            {
                load_failure_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준에 들어오는데 종목 조회 실패
            }
		}
        else {
			DelistingDate_Error_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준 안 맞음
            logger.info('DelistingDate_Error: ' + code[list_index]);
        }
        list_index++;
    }
    // 상장일이 시뮬레이션 시작일보다 빠른 종목들 target_stock에 코드명 맵핑
}

function onDayClose(now){
    var thisYear = now.getYear();
    var thisMonth = now.getMonth();
    var thisDate = now.getDate();
    var today = stock_listed.getDate();

	// today와 같거나 빠른 상장일의 종목들 등록
    while ((ListingDate[list_index] <= today) && (list_index < len_data)){ // 더 최근 날짜가 더 크다
        if (DelistingDate[list_index] >= StartDate) {//DelistingDate 보다 기준일이 더 과거인지 확인
			var _stock = IQStock.getStock(code_modified[list_index]);
            if (_stock != null)
            {
                target_stock[NumOfStocks] = _stock;
                target_stock_code[NumOfStocks] = code[list_index]; // target stock의 original 코드명 맵핑
                target_sector[NumOfStocks] = _stock.sector; // targe stock의 sector
                ListingDate_stock[NumOfStocks] = ListingDate[list_index]; // target stock의 상장일 맵핑
                DelistingDate_stock[NumOfStocks] = DelistingDate[list_index]; // target stock의 상폐일 맵핑
                NumOfStocks++;
                logger.info(_stock.name);
            }
            else
            {
                load_failure_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준에 들어오는데 종목 조회 실패
            }
		}
        else {
			DelistingDate_Error_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준에 안 맞음
        }
        list_index++;
    }

	// 재무 정보 기록
	if( thisMonth == month_quarter_1 && thisDate>=1 && thisYear > last_quarter_1_rebal_year){ // 6월 첫째날
		for (var i=0;  i<NumOfStocks; i++){
			if (DelistingDate_stock[i] >= today){ // 상폐일이 오늘 이후인 경우만 데이터 읽음
			    getData(i);
			}
		}
		last_quarter_1_rebal_year = thisYear;
        last_data_day = today;
	}
    else if( thisMonth == month_quarter_2 && thisDate>=1 && thisYear > last_quarter_2_rebal_year){ // 9월 첫째날
		for (var i=0;  i<NumOfStocks; i++){
			if (DelistingDate_stock[i] >= today){ // 상폐일이 오늘 이후인 경우만 데이터 읽음
			    getData(i);
			}
		}
		last_quarter_2_rebal_year = thisYear;
        last_data_day = today;
	}
    else if( thisMonth == month_quarter_3 && thisDate>=1 && thisYear > last_quarter_3_rebal_year){ // 12월 첫째날
		for (var i=0;  i<NumOfStocks; i++){
			if (DelistingDate_stock[i] >= today){ // 상폐일이 오늘 이후인 경우만 데이터 읽음
				getData(i);
			}
		}
		last_quarter_3_rebal_year = thisYear;
        last_data_day = today;
	}
    else if( thisMonth == month_quarter_4 && thisDate>=1 && thisYear > last_quarter_4_rebal_year){ // 4월 첫째날
		for (var i=0;  i<NumOfStocks; i++){
			if (DelistingDate_stock[i] >= today){ // 상폐일이 오늘 이후인 경우만 데이터 읽음
				getData(i);
			}
		}
		last_quarter_4_rebal_year = thisYear;
        last_data_day = today;
	}

    if (today >= FinalDate){ // 백테스트 마지막날
        for (var i=0;  i<NumOfStocks; i++){
            if( ListingDate_stock[i]>last_data_day){ // 상장일이 마지막 fiancial data 습득일보다 뒤인 경우
				//logger.info(removeA(target_stock_code[i]) + ', rv: 0, gp: 0, oi: 0, np: 0, ev_evitda: 0, per: 0, pbr: 0, psr: 0, pcr: 0, gpa: 0, roa: 0, roe: 0, der: 0, icr: 0, nd_ebitda: 0, fcfy: 0' + ' 상장일이 마지막 fiancial data 습득일보다 뒤');
				logger.info(removeA(target_stock_code[i]) + ', sector: ' + target_stock[i].sector + ', te: 0, cap: 0, rv: 0, cfo: 0, cogs: 0, t_a: 0, np: 0, tl: 0, oi: 0, ie: 0, ev: 0, ebitda: 0, ebit: 0, nl: 0, capex: 0, depre: 0, rnd: 0, inven: 0, ca: 0, cl: 0, r_e: 0'
                            + ' 상장일이 마지막 fiancial data 습득일보다 뒤' );
			}
			else if ( DelistingDate_stock[i] < first_data_day){ // 상폐일이 첫번째 fiancial data 습득일보다 먼저인 경우
				//logger.info(removeA(target_stock_code[i]) + ', rv: 0, gp: 0, oi: 0, np: 0, ev_evitda: 0, per: 0, pbr: 0, psr: 0, pcr: 0, gpa: 0, roa: 0, roe: 0, der: 0, icr: 0, nd_ebitda: 0, fcfy: 0' + '상폐일이 첫번째 fiancial data 습득일보다 먼저');
				logger.info(removeA(target_stock_code[i]) + ', sector: ' + target_stock[i].sector + ', te: 0, cap: 0, rv: 0, cfo: 0, cogs: 0, t_a: 0, np: 0, tl: 0, oi: 0, ie: 0, ev: 0, ebitda: 0, ebit: 0, nl: 0, capex: 0, depre: 0, rnd: 0, inven: 0, ca: 0, cl: 0, r_e: 0'
				             + '상폐일이 첫번째 fiancial data 습득일보다 먼저');
			}
		}
        logger.info('list_index: ' + list_index);
        logger.info('NumOfStocks: ' +  NumOfStocks);
        logger.info('load_failure_list: [' + load_failure_list + ']');
        logger.info('DelistingDate_Error_list: [' + DelistingDate_Error_list + ']');
	}
}

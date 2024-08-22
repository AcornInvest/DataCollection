// Parameters
var target_stock = [];
var ListingDate_stock = [];
var DelistingDate_stock = [];
var code_modified = [];
var target_stock_code =[];
var revenue = [];
var GP = [];
var operating_income =[];
var net_profit = [];
var EV_EVITDA = [];
var PER = [];
var PBR = [];
var PSR = [];
var PCR = [];
var GPA = [];
var ROA = [];
var ROE = [];
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

// EV/EBITDA 구하기.
function get_EV_EBITDA(stock) {
	if(stock.getFundamentalEBITDA() == 0) {return 999999999;}	//EBITDA ==0 이면 EV/EBITDA 999999999 리턴
	 return  stock.getFundamentalEV()*1000000/(stock.getFundamentalEBITDA()*1000*4)  ;
}
//PBR
function get_PBR(stock) {
	if (stock.getFundamentalTotalEquity() == 0) return 999999999;
	// 자본 총계가 0 이면 PBR 을 999999999 정의함. 순위에서 밀리도록 하기 위함
	var pbr_temp  = stock.getMarketCapital()*1000000 / (stock.getFundamentalTotalEquity()*1000);
    //logger.info('pbr equity: ' + stock.getFundamentalTotalEquity());

	if (pbr_temp < 0){ //PBR이 0 미만이면 999999999 리턴. 순위에서 밀리도록.
		return 999999999; //
	}
	else{
		return  pbr_temp; //PBR
    // getMarketCapital() 함수의 리턴값은 단위가 백만원임
	}
}
//PER
function get_PER(stock) {
	if (stock.getFundamentalNetProfit() == 0) {return 999999999;}	 // 순이익이 0 이면 999999999 리턴하여 순위가 밀리도록 함
    return ( stock.getMarketCapital()*1000000 / (stock.getFundamentalNetProfit()*1000*4) );
        // getMarketCapital() 함수의 리턴값은 단위가 백만원임
        // getFundamentalNetProfit(): 최종분기의 당기순이익
}

// PSR 구하기. 시가 총액/매출액
function get_PSR(stock) {
    if (stock.getFundamentalRevenue() == 0) {return 999999999;}	 // 매출액이 0 이면 999999999 리턴하여 순위가 밀리도록 함
	return  stock.getMarketCapital()*1000000/(stock.getFundamentalRevenue()*1000*4);
        // getMarketCapital() 시가총액. 함수의 리턴값은 단위가 백만원임
        // getFundamentalRevenue(): 최종분기의 매출액. 단위가 1000원
}

// PCR 구하기. 시가 총액/영업 현금 흐름
function get_PCR(stock) {
	if (stock.getFundamentalOperatingCashFlow() == 0) {return 999999999;}	 // 영업현금흐름이 0 이면 10000으로 리턴하여 순위가 밀리도록 함
	else
	{
		 return  stock.getMarketCapital()*1000000/(stock.getFundamentalOperatingCashFlow()*1000*4);
        // getMarketCapital() 시가총액. 함수의 리턴값은 단위가 백만원임
        // getFundamentalNetProfit(): 최종분기의 영업활동으로 인한 현금흐름. 단위가 1000원 인가?
	}
}
// GP/A
function get_GPA(stock) {
    var asset = stock.getFundamentalTotalAsset();
    var temp = 4*(stock.getFundamentalRevenue() - stock.getFundamentalSalesCost())/asset;

	if (asset == 0) { // 자산총계가 0인 경우. 재무제표가 누락되었다고 봐야 한다.
        return -999999999;
    }
    else {
        return temp;
    }
}
function get_ROA(stock){ //ROA
    var asset = stock.getFundamentalTotalAsset();
    var net_profit = stock.getFundamentalNetProfit();
    var roa = 4 *net_profit/asset*100; // ROA 는 보통 % 단위로 환산한다.
    //logger.info('function roa: ' + stock.getROA())
   if (asset == 0) { // 자산총계가 0인 경우. 재무제표가 누락되었다고 봐야 한다.
        return -999999999;
    }
    else {
        return roa;
    }
}
function get_ROE(stock){ //ROE
    var equity = stock.getFundamentalTotalEquity();
    var net_profit = stock.getFundamentalNetProfit();
    var roe = (4*net_profit/equity)*100;	// ROE 는 보통 % 단위로 환산하여 본다.
    //logger.info('roe: ' + stock.getROE())
    //var temp = stock.getROE();
    if (equity <=0 ) { //자기자본이 0이하인 경우. 기업 상태를 최악으로 본다. 자기자본이 0인 경우는 재무제표가 누락된 것
        return -999999999;
    }
    /*
    else if (roe == Infinity || roe == -Infinity ){
        logger.info('equity: ' + equity + ', net_profit:' + net_profit );
        return roe;
    }
    */
    else {
        return roe;
    }
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
                ListingDate_stock[NumOfStocks] = ListingDate[list_index]; // target stock의 상장일 맵핑
                DelistingDate_stock[NumOfStocks] = DelistingDate[list_index]; // target stock의 상폐일 맵핑
                NumOfStocks++;
                logger.info('_stock: ' + _stock.name + ', ' + NumOfStocks );
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
				revenue[i] = target_stock[i].getFundamentalRevenue(); // 매출액 [천원]
				GP[i] = target_stock[i].getFundamentalGrossProfit(); // 매출총이익 [천원]
				operating_income[i] = target_stock[i].getFundamentalOperatingIncome(); // 영업이익 [천원]
				net_profit[i] = target_stock[i].getFundamentalNetProfit(); // 당기순이익 [천원]
				EV_EVITDA[i] = roundToDecimal(get_EV_EBITDA(target_stock[i]),2);
				PER[i] = roundToDecimal(get_PER(target_stock[i]),2);
                PBR[i] = roundToDecimal(get_PBR(target_stock[i]),2);
                PSR[i] = roundToDecimal(get_PSR(target_stock[i]),2);
                PCR[i] = roundToDecimal(get_PCR(target_stock[i]),2);
                GPA[i] = roundToDecimal(get_GPA(target_stock[i]),2);
                ROA[i] = roundToDecimal(get_ROA(target_stock[i]),2);
                ROE[i] = roundToDecimal(get_ROE(target_stock[i]),2);
				logger.info(removeA(target_stock_code[i]) + ', rv: ' + revenue[i] + ', gp: ' + GP[i] + ', oi: ' + operating_income[i] + ', np: ' + net_profit[i]
                           + ', ev_evitda: ' +  EV_EVITDA[i] + ', per: ' + PER[i] + ', pbr: ' + PBR[i] + ', psr: ' + PSR[i]
                            + ', pcr: ' +  PCR[i] + ', gpa: ' + GPA[i] + ', roa: ' + ROA[i] + ', roe: ' + ROE[i]);
			}
		}
		last_quarter_1_rebal_year = thisYear;
        last_data_day = today;
	}
    else if( thisMonth == month_quarter_2 && thisDate>=1 && thisYear > last_quarter_2_rebal_year){ // 9월 첫째날
		for (var i=0;  i<NumOfStocks; i++){
			if (DelistingDate_stock[i] >= today){ // 상폐일이 오늘 이후인 경우만 데이터 읽음
				revenue[i] = target_stock[i].getFundamentalRevenue(); // 매출액 [천원]
				GP[i] = target_stock[i].getFundamentalGrossProfit(); // 매출총이익 [천원]
				operating_income[i] = target_stock[i].getFundamentalOperatingIncome(); // 영업이익 [천원]
				net_profit[i] = target_stock[i].getFundamentalNetProfit(); // 당기순이익 [천원]
               	EV_EVITDA[i] = roundToDecimal(get_EV_EBITDA(target_stock[i]),2);
				PER[i] = roundToDecimal(get_PER(target_stock[i]),2);
                PBR[i] = roundToDecimal(get_PBR(target_stock[i]),2);
                PSR[i] = roundToDecimal(get_PSR(target_stock[i]),2);
                PCR[i] = roundToDecimal(get_PCR(target_stock[i]),2);
                GPA[i] = roundToDecimal(get_GPA(target_stock[i]),2);
                ROA[i] = roundToDecimal(get_ROA(target_stock[i]),2);
                ROE[i] = roundToDecimal(get_ROE(target_stock[i]),2);
				logger.info(removeA(target_stock_code[i]) + ', rv: ' + revenue[i] + ', gp: ' + GP[i] + ', oi: ' + operating_income[i] + ', np: ' + net_profit[i]
                           + ', ev_evitda: ' +  EV_EVITDA[i] + ', per: ' + PER[i] + ', pbr: ' + PBR[i] + ', psr: ' + PSR[i]
                            + ', pcr: ' +  PCR[i] + ', gpa: ' + GPA[i] + ', roa: ' + ROA[i] + ', roe: ' + ROE[i]);
			}
		}
		last_quarter_2_rebal_year = thisYear;
        last_data_day = today;
	}
    else if( thisMonth == month_quarter_3 && thisDate>=1 && thisYear > last_quarter_3_rebal_year){ // 12월 첫째날
		for (var i=0;  i<NumOfStocks; i++){
			if (DelistingDate_stock[i] >= today){ // 상폐일이 오늘 이후인 경우만 데이터 읽음
				revenue[i] = target_stock[i].getFundamentalRevenue(); // 매출액 [천원]
				GP[i] = target_stock[i].getFundamentalGrossProfit(); // 매출총이익 [천원]
				operating_income[i] = target_stock[i].getFundamentalOperatingIncome(); // 영업이익 [천원]
				net_profit[i] = target_stock[i].getFundamentalNetProfit(); // 당기순이익 [천원]
				EV_EVITDA[i] = roundToDecimal(get_EV_EBITDA(target_stock[i]),2);
				PER[i] = roundToDecimal(get_PER(target_stock[i]),2);
                PBR[i] = roundToDecimal(get_PBR(target_stock[i]),2);
                PSR[i] = roundToDecimal(get_PSR(target_stock[i]),2);
                PCR[i] = roundToDecimal(get_PCR(target_stock[i]),2);
                GPA[i] = roundToDecimal(get_GPA(target_stock[i]),2);
                ROA[i] = roundToDecimal(get_ROA(target_stock[i]),2);
                ROE[i] = roundToDecimal(get_ROE(target_stock[i]),2);
				logger.info(removeA(target_stock_code[i]) + ', rv: ' + revenue[i] + ', gp: ' + GP[i] + ', oi: ' + operating_income[i] + ', np: ' + net_profit[i]
                           + ', ev_evitda: ' +  EV_EVITDA[i] + ', per: ' + PER[i] + ', pbr: ' + PBR[i] + ', psr: ' + PSR[i]
                            + ', pcr: ' +  PCR[i] + ', gpa: ' + GPA[i] + ', roa: ' + ROA[i] + ', roe: ' + ROE[i]);
			}
		}
		last_quarter_3_rebal_year = thisYear;
        last_data_day = today;
	}
    else if( thisMonth == month_quarter_4 && thisDate>=1 && thisYear > last_quarter_4_rebal_year){ // 4월 첫째날
		for (var i=0;  i<NumOfStocks; i++){
			if (DelistingDate_stock[i] >= today){ // 상폐일이 오늘 이후인 경우만 데이터 읽음
				revenue[i] = target_stock[i].getFundamentalRevenue(); // 매출액 [천원]
				GP[i] = target_stock[i].getFundamentalGrossProfit(); // 매출총이익 [천원]
				operating_income[i] = target_stock[i].getFundamentalOperatingIncome(); // 영업이익 [천원]
				net_profit[i] = target_stock[i].getFundamentalNetProfit(); // 당기순이익 [천원]
				EV_EVITDA[i] = roundToDecimal(get_EV_EBITDA(target_stock[i]),2);
				PER[i] = roundToDecimal(get_PER(target_stock[i]),2);
                PBR[i] = roundToDecimal(get_PBR(target_stock[i]),2);
                PSR[i] = roundToDecimal(get_PSR(target_stock[i]),2);
                PCR[i] = roundToDecimal(get_PCR(target_stock[i]),2);
                GPA[i] = roundToDecimal(get_GPA(target_stock[i]),2);
                ROA[i] = roundToDecimal(get_ROA(target_stock[i]),2);
                ROE[i] = roundToDecimal(get_ROE(target_stock[i]),2);
				logger.info(removeA(target_stock_code[i]) + ', rv: ' + revenue[i] + ', gp: ' + GP[i] + ', oi: ' + operating_income[i] + ', np: ' + net_profit[i]
                           + ', ev_evitda: ' +  EV_EVITDA[i] + ', per: ' + PER[i] + ', pbr: ' + PBR[i] + ', psr: ' + PSR[i]
                            + ', pcr: ' +  PCR[i] + ', gpa: ' + GPA[i] + ', roa: ' + ROA[i] + ', roe: ' + ROE[i]);
			}
		}
		last_quarter_4_rebal_year = thisYear;
        last_data_day = today;
	}

    if (today >= FinalDate){ // 백테스트 마지막날
        for (var i=0;  i<NumOfStocks; i++){
            if( ListingDate_stock[i]>last_data_day){ // 상장일이 마지막 fiancial data 습득일보다 뒤인 경우
				logger.info(removeA(target_stock_code[i]) + ', rv: 0, gp: 0, oi: 0, np: 0, ev_evitda: 0, per: 0, pbr: 0, psr: 0, pcr: 0, gpa: 0, roa: 0, roe: 0' + ' 상장일이 마지막 fiancial data 습득일보다 뒤');
			}
			else if ( DelistingDate_stock[i] < first_data_day){ // 상폐일이 첫번째 fiancial data 습득일보다 먼저인 경우
				logger.info(removeA(target_stock_code[i]) + ', rv: 0, gp: 0, oi: 0, np: 0, ev_evitda: 0, per: 0, pbr: 0, psr: 0, pcr: 0, gpa: 0, roa: 0, roe: 0' + '상폐일이 첫번째 fiancial data 습득일보다 먼저');
			}
		}
        logger.info('list_index: ' + list_index);
        logger.info('NumOfStocks: ' +  NumOfStocks);
        logger.info('load_failure_list: [' + load_failure_list + ']');
        logger.info('DelistingDate_Error_list: [' + DelistingDate_Error_list + ']');
	}
}

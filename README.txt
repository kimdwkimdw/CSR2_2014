- API.py
메인 프로그램

CSR2_API Class가 모든 서버와의 request와 caching을 담당.

CSR2_Scheduler Class는 
schedule1~schedule5라는 이름의 함수를 가지고 있는데 트레이닝기간에 다양한 방법으로 Ad를 media에 배분하는 전략을 가진 함수들
real_schedule1, real_training, real_schedule2 세개의 함수는 실제 테스트기간에 Ad를 media에 배분하는 다양한 전략을 시도해본 함수들

최종적으로는 real_schedule3함수를 사용하여 자원을 배분했었는데 
3번의 turn의 데이터를 그래프로 그려서 plot, plot4, plot5,
click률에대한 분석을 진행한후 클릭률에 대한 패턴을 파악한후 작성한 함수이다.

- simulator.py
분석된 패턴을 로컬에서 시뮬레이션 해보기위한 도구

- money_calculator.py
다양한 방법으로 분석하기 위해 사용했던 스크립트 조각들
그림을 그릴때도 사용하였음.


--------코드 저장소
https://github.com/kimdwkimdw/CSR2_2014
k# 1. 기본 이미지 지정: 작고 안정적인 Python 3.10-slim 이미지 사용
FROM python:3.10-slim

# 2. 작업 디렉토리 설정: 모든 파일이 이 디렉토리 안에 위치합니다.
WORKDIR /app

# 3. 비특권 사용자(non-root user) 생성: 보안 강화를 위해 권장됩니다.
RUN useradd -m -d /app appuser

# 4. 의존성 파일 복사 및 설치: 캐싱 활용을 위해 requirements.txt를 먼저 복사합니다.
COPY requirements.txt .

# 5. 의존성 설치: requirements.txt에 명시된 모든 패키지를 설치합니다.
RUN pip install --no-cache-dir -r requirements.txt

# 6. 나머지 모든 파일 복사: server.py, 기타 파일을 컨테이너에 복사합니다.
COPY . .

# 7. 파일 소유권 변경: appuser가 /app 디렉토리를 소유하도록 합니다.
RUN chown -R appuser:appuser /app

# 8. 사용자 전환: 서버 실행 시 appuser로 실행하여 보안을 강화합니다.
USER appuser

# 9. 포트 노출: 서버가 통신할 포트(8000)를 외부에 알립니다.
EXPOSE 8100

# 10. 서버 실행 명령어: 서버가 시작될 때 실행할 최종 명령입니다.
#     uvicorn [모듈 이름]:[인스턴스 이름] (server:app)을 사용하여 서버를 구동합니다.
CMD ["uvicorn", "cctvapi.server:app", "--host", "0.0.0.0", "--port", "8100"]
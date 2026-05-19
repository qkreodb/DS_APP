package com.example.ds_safer.ui.screens.floormap

import android.util.Log
import androidx.lifecycle.ViewModel
import retrofit2.HttpException
import androidx.lifecycle.viewModelScope
import com.example.ds_safer.data.api.RetrofitClient
import com.example.ds_safer.data.repository.AlertRepository
import com.example.ds_safer.data.repository.JetsonRepository
import com.example.ds_safer.domain.model.AvailableCctvDto
import com.example.ds_safer.domain.model.FloorMapInfo
import com.example.ds_safer.domain.model.RecentAlertDto
import com.example.ds_safer.domain.model.RegisteredSensor
import com.example.ds_safer.domain.model.SaveSensorPositionRequest
import com.example.ds_safer.domain.model.SensorMapPosition
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

class FloorMapViewModel : ViewModel() {

    private val _floorMap = MutableStateFlow<FloorMapInfo?>(null)
    val floorMap = _floorMap.asStateFlow()

    // AlertRepository StateFlow를 직접 노출 → WebSocket 수신 시 자동 갱신
    val recentAlerts = AlertRepository.alerts

    private val _availableSensors = MutableStateFlow<List<RegisteredSensor>>(emptyList())
    val availableSensors = _availableSensors.asStateFlow()

    private val _availableCctvs = MutableStateFlow<List<AvailableCctvDto>>(emptyList())
    val availableCctvs = _availableCctvs.asStateFlow()

    private val _placedSensors = MutableStateFlow<List<SensorMapPosition>>(emptyList())
    val placedSensors = _placedSensors.asStateFlow()

    private val _selectedSensor = MutableStateFlow<RegisteredSensor?>(null)
    val selectedSensor = _selectedSensor.asStateFlow()

    private val _isLoading = MutableStateFlow(false)
    val isLoading = _isLoading.asStateFlow()

    private val _message = MutableStateFlow<String?>(null)
    val message = _message.asStateFlow()

    fun selectSensor(sensor: RegisteredSensor?) {
        _selectedSensor.value = sensor
    }

    fun clearMessage() {
        _message.value = null
    }

    fun loadAll() {
        val device = JetsonRepository.selectedJetson.value

        if (device == null) {
            _floorMap.value = null
            _availableSensors.value = emptyList()
            _placedSensors.value = emptyList()
            _message.value = "선택된 Jetson 정보가 없습니다."
            return
        }

        val spaceId = device.spaceId

        if (spaceId == null || spaceId <= 0) {
            _floorMap.value = null
            _availableSensors.value = emptyList()
            _placedSensors.value = emptyList()
            _message.value = "Jetson에 등록된 공간 정보가 없습니다."
            return
        }

        viewModelScope.launch {
            _isLoading.value = true
            _message.value = null

            val baseUrl = "http://${device.ipAddress}:${device.port}/"
            Log.d("FloorMapVM", "loadAll start | device=${device.name} ip=${device.ipAddress}:${device.port} spaceId=$spaceId")

            try {
                val service = RetrofitClient.createService(baseUrl)

                // ── 1. 평면도 조회 ──────────────────────────────────────────
                try {
                    val mapResponse = service.getFloorMapBySpaceId(spaceId)
                    if (mapResponse.status == "success" && mapResponse.data != null) {
                        _floorMap.value = mapResponse.data
                        Log.d("FloorMapVM", "floorMap OK mapId=${mapResponse.data.mapId} spaceName=${mapResponse.data.spaceName}")
                    } else {
                        _floorMap.value = null
                        _placedSensors.value = emptyList()
                        Log.w("FloorMapVM", "floorMap response status=${mapResponse.status} data=${mapResponse.data}")
                        _message.value = "이 공간에 등록된 평면도가 없습니다. (spaceId=$spaceId)"
                    }
                } catch (e: retrofit2.HttpException) {
                    _floorMap.value = null
                    _placedSensors.value = emptyList()
                    val code = e.code()
                    Log.e("FloorMapVM", "floorMap HTTP $code spaceId=$spaceId url=${baseUrl}api/maps/space/$spaceId")
                    _message.value = when (code) {
                        404 -> "이 공간(spaceId=$spaceId)에 등록된 평면도가 없습니다. 서버 DB를 확인하세요."
                        else -> "평면도 조회 실패 (HTTP $code)"
                    }
                } catch (e: Exception) {
                    _floorMap.value = null
                    _placedSensors.value = emptyList()
                    Log.e("FloorMapVM", "floorMap error spaceId=$spaceId", e)
                    _message.value = "평면도 조회 오류: ${e.message}"
                }

                val mapId = _floorMap.value?.mapId
                Log.d("FloorMapVM", "mapId=$mapId")

                // ── 2. 배치 가능 온습도 센서 ──────────────────────────────────
                try {
                    val sensorResponse = service.getAvailableTempSensorsForMapBySpace(spaceId, mapId)
                    _availableSensors.value = if (sensorResponse.status == "success") sensorResponse.data else emptyList()
                    Log.d("FloorMapVM", "availableSensors count=${_availableSensors.value.size}")
                } catch (e: Exception) {
                    _availableSensors.value = emptyList()
                    Log.w("FloorMapVM", "availableSensors error: ${e.message}")
                }

                // ── 3. 배치 가능 CCTV (demo CCTV 포함) ─────────────────────
                try {
                    val cctvResponse = service.getAvailableCctvsForMapBySpace(spaceId, mapId)
                    // _availableCctvs.value = if (cctvResponse.status == "success") cctvResponse.data else emptyList()
                    _availableCctvs.value = cctvResponse.data
                    Log.d("[TEMP] _availableCctvs.value=${_availableCctvs.value}")
                    Log.d("FloorMapVM", "availableCctvs count=${_availableCctvs.value.size} items=${_availableCctvs.value.map { "${it.senName}(demo=${it.isDemo})" }}")
                } catch (e: Exception) {
                    _availableCctvs.value = emptyList()
                    Log.e("FloorMapVM", "availableCctvs error spaceId=$spaceId mapId=$mapId url=${baseUrl}api/maps/space/$spaceId/available-cctvs?map_id=$mapId", e)
                }

                // ── 4. 배치된 센서/CCTV 위치 ──────────────────────────────
                if (mapId != null) {
                    try {
                        val placedResponse = service.getMapSensorPositions(mapId)
                        _placedSensors.value = if (placedResponse.status == "success") placedResponse.data else emptyList()
                        Log.d("FloorMapVM", "placedSensors count=${_placedSensors.value.size}")
                    } catch (e: Exception) {
                        _placedSensors.value = emptyList()
                        Log.w("FloorMapVM", "placedSensors error mapId=$mapId: ${e.message}")
                    }
                } else {
                    _placedSensors.value = emptyList()
                }

                // ── 5. 최근 알림 ───────────────────────────────────────────
                try {
                    AlertRepository.setAlerts(service.getRecentAlerts(spaceId, 20).data)
                } catch (_: Exception) {}

            } catch (e: Exception) {
                Log.e("FloorMapVM", "loadAll unexpected error", e)
                _floorMap.value = null
                _availableSensors.value = emptyList()
                _placedSensors.value = emptyList()
                _message.value = "서버 연결 오류: ${e.message}"
            } finally {
                _isLoading.value = false
            }
        }
    }

    /**
     * 기존 방식 유지:
     * 화면에서 선택된 센서를 기준으로 저장
     */
    fun saveSensorPosition(xRatio: Float, yRatio: Float) {
        val selected = _selectedSensor.value

        if (selected == null) {
            _message.value = "먼저 온습도 센서를 선택해주세요."
            return
        }

        val mapInfo = _floorMap.value

        if (mapInfo == null) {
            _message.value = "평면도 정보가 없습니다."
            return
        }

        saveSensorPosition(
            mapId = mapInfo.mapId,
            sensorId = selected.sensorId,
            xRatio = xRatio,
            yRatio = yRatio
        )
    }

    /**
     * 새 화면 코드에서 직접 호출할 수 있는 방식.
     * map_id 기준으로 위치 저장.
     */
    fun saveSensorPosition(
        mapId: Int,
        sensorId: String,
        xRatio: Float,
        yRatio: Float
    ) {
        val device = JetsonRepository.selectedJetson.value

        if (device == null) {
            _message.value = "선택된 Jetson 정보가 없습니다."
            return
        }

        viewModelScope.launch {
            _isLoading.value = true
            _message.value = null

            try {
                val baseUrl = "http://${device.ipAddress}:${device.port}/"
                val service = RetrofitClient.createService(baseUrl)

                val response = service.saveSensorPosition(
                    SaveSensorPositionRequest(
                        mapId = mapId,
                        sensorId = sensorId,
                        xRatio = xRatio,
                        yRatio = yRatio
                    )
                )

                if (response.status == "success") {
                    _message.value = "센서 위치 저장 완료"

                    val placedResponse = service.getMapSensorPositions(mapId)
                    if (placedResponse.status == "success") {
                        _placedSensors.value = placedResponse.data
                    }

                    val spaceId = device.spaceId
                    if (spaceId != null && spaceId > 0) {
                        val sensorResponse = service.getAvailableTempSensorsForMapBySpace(
                            spaceId = spaceId,
                            mapId = mapId
                        )
                        if (sensorResponse.status == "success") {
                            _availableSensors.value = sensorResponse.data
                        }

                        try {
                            val cctvResponse = service.getAvailableCctvsForMapBySpace(
                                spaceId = spaceId,
                                mapId = mapId
                            )
                            if (cctvResponse.status == "success") {
                                _availableCctvs.value = cctvResponse.data
                            }
                        } catch (_: Exception) {}
                    }

                    _selectedSensor.value = null
                } else {
                    _message.value = response.message
                }
            } catch (e: Exception) {
                Log.e("FloorMapVM", "saveSensorPosition error", e)
                _message.value = "센서 위치 저장 실패"
            } finally {
                _isLoading.value = false
            }
        }
    }
}
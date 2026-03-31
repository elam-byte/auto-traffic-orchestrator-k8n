import * as THREE from 'three';
import type { WorldSnapshot } from '@shared/types';

const MAX_VEHICLES = 2000;
const BODY_H    = 0.8;
const CABIN_H   = 0.7;
const CABIN_L   = 0.55;
const CABIN_W   = 0.90;
const CABIN_FWD = 0.10;

export class VehicleInstancer {
  readonly bodyMesh: THREE.InstancedMesh;
  readonly cabinMesh: THREE.InstancedMesh;

  private dummy        = new THREE.Object3D();
  private idToIndex    = new Map<string, number>();
  private nextIndex    = 0;
  private colorCache   = new Map<string, THREE.Color>();
  private defaultColor = new THREE.Color('#22c55e');

  constructor(scene: THREE.Scene) {
    const bodyGeo = new THREE.BoxGeometry(1, 1, 1);
    const bodyMat = new THREE.MeshStandardMaterial({ roughness: 0.5, metalness: 0.3 });
    this.bodyMesh = new THREE.InstancedMesh(bodyGeo, bodyMat, MAX_VEHICLES);
    this.bodyMesh.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
    this.bodyMesh.count = 0;
    this.bodyMesh.setColorAt(0, this.defaultColor);
    if (this.bodyMesh.instanceColor) {
      this.bodyMesh.instanceColor.setUsage(THREE.DynamicDrawUsage);
    }
    this.bodyMesh.frustumCulled = false;

    const cabinGeo = new THREE.BoxGeometry(1, 1, 1);
    const cabinMat = new THREE.MeshStandardMaterial({ color: 0x1a1a2e, roughness: 0.4, metalness: 0.5 });
    this.cabinMesh = new THREE.InstancedMesh(cabinGeo, cabinMat, MAX_VEHICLES);
    this.cabinMesh.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
    this.cabinMesh.count = 0;
    this.cabinMesh.frustumCulled = false;

    const zero = new THREE.Matrix4().makeScale(0, 0, 0);
    for (let i = 0; i < MAX_VEHICLES; i++) {
      this.bodyMesh.setMatrixAt(i, zero);
      this.cabinMesh.setMatrixAt(i, zero);
    }
    this.bodyMesh.instanceMatrix.needsUpdate  = true;
    this.cabinMesh.instanceMatrix.needsUpdate = true;

    scene.add(this.bodyMesh);
    scene.add(this.cabinMesh);
  }

  updateFromSnapshot(snapshot: WorldSnapshot): void {
    for (const v of snapshot.vehicles) {
      let idx = this.idToIndex.get(v.id);
      if (idx === undefined) {
        if (this.nextIndex >= MAX_VEHICLES) continue;
        idx = this.nextIndex++;
        this.idToIndex.set(v.id, idx);
      }

      // Body
      this.dummy.position.set(v.x, v.y, BODY_H / 2);
      this.dummy.rotation.set(0, 0, v.heading);
      this.dummy.scale.set(v.length, v.width, BODY_H);
      this.dummy.updateMatrix();
      this.bodyMesh.setMatrixAt(idx, this.dummy.matrix);

      // Cabin (offset toward front)
      const fwdX = Math.cos(v.heading) * v.length * CABIN_FWD;
      const fwdY = Math.sin(v.heading) * v.length * CABIN_FWD;
      this.dummy.position.set(v.x + fwdX, v.y + fwdY, BODY_H + CABIN_H / 2);
      this.dummy.scale.set(v.length * CABIN_L, v.width * CABIN_W, CABIN_H);
      this.dummy.updateMatrix();
      this.cabinMesh.setMatrixAt(idx, this.dummy.matrix);

      // Color
      const colorHex = v.color ?? '#22c55e';
      let color = this.colorCache.get(colorHex);
      if (!color) {
        color = new THREE.Color(colorHex);
        this.colorCache.set(colorHex, color);
      }
      this.bodyMesh.setColorAt(idx, color);
    }

    this.bodyMesh.count  = this.nextIndex;
    this.cabinMesh.count = this.nextIndex;
    this.bodyMesh.instanceMatrix.needsUpdate  = true;
    this.cabinMesh.instanceMatrix.needsUpdate = true;
    if (this.bodyMesh.instanceColor) {
      this.bodyMesh.instanceColor.needsUpdate = true;
    }
  }

  reset(): void {
    this.idToIndex.clear();
    this.nextIndex = 0;
    this.bodyMesh.count  = 0;
    this.cabinMesh.count = 0;
  }

  dispose(): void {
    this.bodyMesh.geometry.dispose();
    this.cabinMesh.geometry.dispose();
    (this.bodyMesh.material  as THREE.Material).dispose();
    (this.cabinMesh.material as THREE.Material).dispose();
  }
}

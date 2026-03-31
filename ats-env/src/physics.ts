import type { VehicleState, VehicleCommand } from "./types.js";

const ACCEL_MIN = -4;
const ACCEL_MAX = 2;
const STEER_MIN = -0.5;
const STEER_MAX = 0.5;
const SPEED_MIN = 0;

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

export function integrate(
  state: VehicleState,
  cmd: VehicleCommand,
  dt: number,
  speedLimit: number
): Pick<VehicleState, "x" | "y" | "heading" | "speed"> {
  const accel = clamp(cmd.desired_accel, ACCEL_MIN, ACCEL_MAX);
  const steer = clamp(cmd.desired_steer, STEER_MIN, STEER_MAX);
  const wheelbase = state.length * 0.6;

  const speed = state.speed;
  const heading = state.heading;

  const newX = state.x + speed * Math.cos(heading) * dt;
  const newY = state.y + speed * Math.sin(heading) * dt;
  const newHeading = heading + (speed / wheelbase) * Math.tan(steer) * dt;
  const newSpeed = clamp(speed + accel * dt, SPEED_MIN, speedLimit);

  return { x: newX, y: newY, heading: newHeading, speed: newSpeed };
}
